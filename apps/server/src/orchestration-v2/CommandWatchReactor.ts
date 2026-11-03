import {
  CommandId,
  MessageId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Notification,
  type OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
  type ThreadCommandWatch,
  type ThreadId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { listLoginShellCandidates } from "@t3tools/shared/shell";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FiberMap from "effect/FiberMap";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { forkParked } from "../serverActivation.ts";
import {
  COMMAND_WATCH_BACKLOG_LIMIT_BYTES,
  COMMAND_WATCH_DELIVERY_FAILURE_LIMIT,
  COMMAND_WATCH_FAILURE_LIMIT,
  COMMAND_WATCH_FAILURE_NOTICE_AT,
  type CommandWatchRun,
  commandWatchFailingNotice,
  commandWatchOutputLimitBytes,
  commandWatchPauseNotice,
  commandWatchRevisionText,
  commandWatchUpdate,
  compareCommandWatchOutput,
  comparisonValue,
  parseComparisonState,
  readComparisonResult,
  readCommandWatchRun,
} from "./commandWatch.ts";
import * as CommandWatchComparison from "./CommandWatchComparison.ts";
import * as CommandWatchStore from "./CommandWatchStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

/** Checks at once across the environment. */
const CONCURRENT_RUNS = 4;
const RUN_TIMEOUT_SECONDS = 60;
/** How often watched threads are looked up while any exist, and when none do. */
const ACTIVE_SCAN_MS = 30_000;
const IDLE_SCAN_MS = 5 * 60_000;
/** Storage the agent tool wrote just before starting its watch is not an orphan yet. */
const ORPHAN_GRACE_MS = 10 * 60_000;
/** Retry delays grow with failures in a row, up to this many intervals. */
const MAX_BACKOFF_INTERVALS = 8;

type SyncPatch = Extract<
  OrchestrationV2ServerCommand,
  { readonly type: "thread.command-watch.sync" }
>["patch"];

interface Wake {
  readonly messageId: MessageId;
  readonly text: string;
  readonly notification: OrchestrationV2Notification;
}

const watchKey = (watch: ThreadCommandWatch) => `${watch.id} ${watch.startedAt}`;

const running = (watch: ThreadCommandWatch) =>
  watch.status === "baselining" || watch.status === "watching";

/** Only an unsettled thread that is not a subagent runs watches. */
const mayRun = (
  thread: Pick<OrchestrationV2AppThread, "lineage" | "settledOverride" | "settledAt">,
) =>
  thread.lineage.relationshipToParent !== "subagent" &&
  thread.settledOverride !== "settled" &&
  thread.settledAt === null;

/** The agent never saw this run's message: the run failed before it started working. */
const neverReachedAgent = (run: Pick<OrchestrationV2Run, "status" | "startedAt">) =>
  run.status === "failed" && run.startedAt === null;

/** The update message ends with the last change it covers, so a restart can tell what it held. */
const updateMessageId = (watch: ThreadCommandWatch, attempt: string, through: number) =>
  MessageId.make(`message:command-watch:${watch.id}:${attempt}:${through}`);
const throughOf = (messageId: string) => Number(messageId.slice(messageId.lastIndexOf(":") + 1));

/**
 * The server's own shell, run non-interactively: the server already loaded the login PATH.
 * Resolved once, since finding the user's shell can read the account database.
 */
export const resolveCommandWatchShell = Effect.gen(function* () {
  const platform = yield* HostProcess.Platform;
  if (platform === "win32") {
    return {
      program: "powershell.exe",
      invocation: (command: string) => ["-NoProfile", "-NonInteractive", "-Command", command],
    };
  }
  const environment = yield* HostProcess.Environment;
  return {
    program: listLoginShellCandidates(platform, environment.SHELL)[0] ?? "/bin/sh",
    invocation: (command: string) => ["-c", command],
  };
});

/**
 * Runs each watch's command on its interval, keeps going while the agent is busy, and delivers
 * detected changes one bounded update at a time. Due times live in memory, so a restart checks
 * each watch at the next look; everything a check accepted is in storage first.
 */
export class CommandWatchReactor extends Context.Service<
  CommandWatchReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    /** One look for due watches and waiting updates. Starts their work without waiting for it. */
    readonly sweep: Effect.Effect<void>;
    /** Waits until no check is in flight. */
    readonly awaitRuns: Effect.Effect<void>;
  }
>()("t3/orchestration-v2/CommandWatchReactor") {}

/** @public */
export const make = Effect.gen(function* () {
  const engine = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const store = yield* CommandWatchStore.CommandWatchStore;
  const processes = yield* ProcessRunner.ProcessRunner;
  const comparison = yield* CommandWatchComparison.CommandWatchComparison;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const permits = yield* Semaphore.make(CONCURRENT_RUNS);
  const shell = yield* resolveCommandWatchShell;
  const runs = yield* FiberMap.make<string, void, never>();
  const dueAt = new Map<string, number>();
  /** When an update that failed to reach the agent may go again. A restart may retry sooner. */
  const deliveryRetryAt = new Map<string, number>();
  let watchesChanged = true;
  let nextScanAt = 0;

  const sha256 = (text: string) =>
    crypto.digest("SHA-256", new TextEncoder().encode(text)).pipe(Effect.map(Hex.encode));

  const record = Effect.fn("CommandWatchReactor.record")(function* (
    threadId: ThreadId,
    watch: ThreadCommandWatch,
    patch: SyncPatch,
    wake?: Wake,
  ) {
    // Never derived from what it carries: a refused command id stays refused, so an update refused
    // while the thread settled could never go out. Its pending marker prevents a second queueing.
    yield* engine.dispatch({
      type: "thread.command-watch.sync",
      commandId: CommandId.make(`server:command-watch:${watch.id}:${yield* crypto.randomUUIDv4()}`),
      threadId,
      watchId: watch.id,
      startedAt: watch.startedAt,
      patch,
      ...(wake === undefined ? {} : { wake }),
    });
  });

  /**
   * Notices what became of the queued update, then queues the next one from waiting changes.
   * Returns the patch and update to record; changes count as delivered once their update reached
   * the agent, or the user removed it from the queue.
   */
  const planDelivery = Effect.fn("CommandWatchReactor.planDelivery")(function* (
    threadId: ThreadId,
    watch: ThreadCommandWatch,
  ) {
    const patch: { -readonly [K in keyof SyncPatch]: SyncPatch[K] } = {};
    const now = yield* Clock.currentTimeMillis;
    const readRuns = projections
      .getThreadRecords(threadId, ["runs"])
      .pipe(Effect.map(({ runs: threadRuns }) => threadRuns));
    let deliveryFailures = watch.deliveryFailures ?? 0;
    if (watch.pendingMessageId !== undefined) {
      const run = (yield* readRuns).find(
        (candidate) => candidate.userMessageId === watch.pendingMessageId,
      );
      if (
        run !== undefined &&
        (run.status === "queued" || run.status === "preparing" || run.status === "starting")
      ) {
        return { patch, wake: undefined };
      }
      patch.pendingMessageId = null;
      if (run !== undefined && neverReachedAgent(run)) {
        deliveryFailures += 1;
        patch.deliveryFailures = deliveryFailures;
        patch.lastFailure = "could not start the agent on an update";
        deliveryRetryAt.set(watch.id, now + watch.everyMinutes * 60_000);
        if (deliveryFailures >= COMMAND_WATCH_DELIVERY_FAILURE_LIMIT && running(watch)) {
          patch.status = "paused";
          patch.pauseReason = "failures";
        }
      } else {
        yield* store.markDelivered(watch.id, throughOf(watch.pendingMessageId));
        if (deliveryFailures !== 0) patch.deliveryFailures = 0;
        deliveryFailures = 0;
      }
    }
    const waiting = yield* store.waiting(watch.id);
    if (
      waiting.length === 0 ||
      deliveryFailures >= COMMAND_WATCH_DELIVERY_FAILURE_LIMIT ||
      (deliveryRetryAt.get(watch.id) ?? 0) > now
    ) {
      if (watch.waitingChanges !== waiting.length) patch.waitingChanges = waiting.length;
      return { patch, wake: undefined };
    }
    const prefix = `message:command-watch:${watch.id}:`;
    const recentUpdates = (yield* readRuns).filter(
      (run) =>
        run.userMessageId.startsWith(prefix) &&
        DateTime.toEpochMillis(run.requestedAt) > now - 24 * 60 * 60_000,
    ).length;
    const update = commandWatchUpdate(watch, waiting, recentUpdates + 1);
    if (update === undefined) return { patch, wake: undefined };
    const messageId = updateMessageId(watch, yield* crypto.randomUUIDv4(), update.through);
    patch.pendingMessageId = messageId;
    patch.waitingChanges = update.remaining;
    return {
      patch,
      wake: { messageId, text: update.text, notification: update.notification } satisfies Wake,
    };
  });

  /**
   * The revision this server would actually run now. It covers this environment's id, so a
   * database copied elsewhere carries no grants, and the stored helper and comparison copies.
   */
  const currentRevision = Effect.fn("CommandWatchReactor.currentRevision")(function* (
    thread: ProjectionStore.ProjectionThreadCommandWatches,
    watch: ThreadCommandWatch,
    storage: CommandWatchStore.CommandWatchStorage,
  ) {
    const helpers = yield* Effect.forEach(storage.helpers, (helper) =>
      sha256(helper.content).pipe(Effect.map((sha) => ({ path: helper.path, sha256: sha }))),
    );
    const revisionHash = yield* sha256(
      commandWatchRevisionText({
        environmentId: yield* environment.getEnvironmentId,
        threadId: thread.id,
        command: watch.command,
        format: watch.format,
        cwd: watch.cwd,
        shell: shell.program,
        comparisonSourceSha256:
          storage.comparisonSource === null ? null : yield* sha256(storage.comparisonSource),
        completion: watch.completion,
        everyMinutes: watch.everyMinutes,
        reportInitialState: watch.reportInitialState,
        helpers,
      }),
    );
    return { revisionHash, helpers };
  });

  const execute = Effect.fn("CommandWatchReactor.execute")(function* (
    watch: ThreadCommandWatch,
    helpers: ReadonlyArray<CommandWatchStore.CommandWatchHelperFile>,
    hasComparison: boolean,
  ) {
    if (!(yield* fileSystem.exists(watch.cwd).pipe(Effect.orElseSucceed(() => false)))) {
      return {
        kind: "failed",
        reason: "could not run: its working directory is missing",
      } satisfies CommandWatchRun;
    }
    // The stored helper copies, so editing the workspace files cannot change what runs.
    const helperDirectory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-watch-" });
    yield* Effect.forEach(
      helpers,
      (helper) =>
        Effect.gen(function* () {
          const target = path.join(helperDirectory, helper.path);
          yield* fileSystem.makeDirectory(path.dirname(target), { recursive: true });
          yield* fileSystem.writeFileString(target, helper.content);
          yield* fileSystem.chmod(target, 0o700);
        }),
      { discard: true },
    );
    return yield* processes
      .run({
        command: shell.program,
        args: shell.invocation(watch.command),
        cwd: watch.cwd,
        env: { T3_WATCH_HELPERS: helperDirectory },
        timeout: `${RUN_TIMEOUT_SECONDS} seconds`,
        timeoutBehavior: "timedOutResult",
        // Beyond the limit, so an over-long result reads as too long, not as cut.
        maxOutputBytes: commandWatchOutputLimitBytes(hasComparison) * 2,
        outputMode: "truncate",
      })
      .pipe(
        Effect.map((output): CommandWatchRun =>
          output.timedOut
            ? { kind: "failed", reason: `timed out after ${RUN_TIMEOUT_SECONDS} seconds` }
            : output.code === null
              ? { kind: "failed", reason: "was stopped by a signal" }
              : {
                  kind: "exited",
                  code: output.code,
                  stdout: output.stdout,
                  stderr: output.stderr,
                  stdoutTruncated: output.stdoutTruncated,
                },
        ),
        Effect.catch((error) =>
          Effect.succeed<CommandWatchRun>({
            kind: "failed",
            reason: `could not start: ${error.message}`,
          }),
        ),
      );
  }, Effect.scoped);

  /** Agent-written comparison, or the built-in one. Fails as a check, never by defect. */
  const compare = Effect.fn("CommandWatchReactor.compare")(function* (
    watch: ThreadCommandWatch,
    storage: CommandWatchStore.CommandWatchStorage,
    run: CommandWatchRun,
  ) {
    const observed = readCommandWatchRun({
      format: watch.format,
      run,
      hasComparison: storage.comparisonSource !== null,
    });
    if (observed.kind === "failed") return observed;
    if (storage.comparisonSource === null) {
      return compareCommandWatchOutput({
        format: watch.format,
        previous: storage.observation,
        reportInitialState: watch.reportInitialState,
        observed,
      });
    }
    // Identical output skips comparison, so a quiet check spawns nothing.
    if (observed.observation === storage.observation) return { kind: "unchanged" } as const;
    const phase = storage.observation === null ? ("baseline" as const) : ("update" as const);
    return yield* comparison
      .compare(storage.comparisonSource, {
        phase,
        previous: comparisonValue(storage.observation, watch.format),
        current: observed.value,
        state: parseComparisonState(storage.comparisonState),
      })
      .pipe(
        Effect.map((resultJson) =>
          readComparisonResult({
            resultJson,
            observation: observed.observation,
            phase,
            reportInitialState: watch.reportInitialState,
            completion: watch.completion,
          }),
        ),
        Effect.catch((error) =>
          Effect.succeed({
            kind: "failed" as const,
            stage: "comparison" as const,
            reason: `comparison ${error.reason}`,
          }),
        ),
      );
  });

  /** One check: run, compare, store what was accepted, then record and deliver in one command. */
  const check = Effect.fn("CommandWatchReactor.check")(function* (threadId: ThreadId, key: string) {
    // Rechecked after waiting for a permit: the watch may have stopped or lost its authority.
    const [thread] = yield* projections.getThreadsWithCommandWatches(threadId);
    const watch = thread?.commandWatches.find((candidate) => watchKey(candidate) === key);
    if (thread === undefined || watch === undefined || !running(watch) || !mayRun(thread)) return;
    const now = DateTime.formatIso(yield* DateTime.now);
    let storage = yield* store.get(watch.id);
    if (storage === undefined) {
      yield* store.create({ watchId: watch.id, threadId: thread.id, helpers: [], now });
      storage = yield* store.get(watch.id);
      if (storage === undefined) return;
    }
    if (watch.authority === null) return;
    if (watch.authority.kind === "full-access") {
      // Leaving Full access already sent this watch back for approval; this check lost a race.
      if (thread.runtimeMode !== "full-access") return;
    } else {
      const revision = yield* currentRevision(thread, watch, storage);
      const allowed = watch.authority.revisionHash;
      if (revision.revisionHash !== allowed || allowed !== watch.revisionHash) {
        // A changed definition, shell or helper never runs under an approval given for another.
        // The user reviews what would run now; the watch keeps its accepted state.
        yield* record(thread.id, watch, {
          status: "awaiting_approval",
          revisionHash: revision.revisionHash,
          shell: shell.program,
          helpers: revision.helpers,
        });
        return;
      }
    }
    // A result that completed the watch is already stored, so a crash before this recorded it
    // must not be lost to the next check finding the output identical.
    if (storage.completionReason !== null) {
      const delivery = yield* planDelivery(thread.id, watch);
      yield* record(
        thread.id,
        watch,
        {
          ...delivery.patch,
          status: "ended",
          endReason: "completed",
          endDetail: storage.completionReason,
        },
        delivery.wake,
      );
      return;
    }
    const outcome = yield* compare(
      watch,
      storage,
      yield* execute(watch, storage.helpers, storage.comparisonSource !== null),
    );
    const patch: { -readonly [K in keyof SyncPatch]: SyncPatch[K] } = {};
    let failures = 0;
    if (outcome.kind === "failed") {
      const collectionFailures = outcome.stage === "collection" ? watch.collectionFailures + 1 : 0;
      const comparisonFailures =
        outcome.stage === "comparison" ? watch.comparisonFailures + 1 : watch.comparisonFailures;
      failures = Math.max(collectionFailures, comparisonFailures);
      patch.collectionFailures = collectionFailures;
      patch.comparisonFailures = comparisonFailures;
      patch.lastFailure = outcome.reason;
      if (failures >= COMMAND_WATCH_FAILURE_LIMIT) {
        yield* store.note({
          watchId: watch.id,
          change: commandWatchPauseNotice("failures", outcome.reason, now),
          now,
        });
        patch.status = "paused";
        patch.pauseReason = "failures";
      } else {
        // Per stage: a comparison streak standing still must not repeat this on collection failures.
        const inARow = outcome.stage === "collection" ? collectionFailures : comparisonFailures;
        if (inARow === COMMAND_WATCH_FAILURE_NOTICE_AT) {
          yield* store.note({
            watchId: watch.id,
            change: commandWatchFailingNotice(inARow, outcome.reason),
            now,
          });
        }
      }
    } else {
      if (outcome.kind === "accepted") {
        yield* store.accept({
          watchId: watch.id,
          observation: outcome.observation,
          changes: outcome.changes,
          comparisonState: outcome.state,
          completionReason: outcome.completionReason,
          now,
        });
        if (outcome.completionReason !== undefined) {
          patch.status = "ended";
          patch.endReason = "completed";
          patch.endDetail = outcome.completionReason;
        }
        if (outcome.changes.length > 0) patch.lastChangeAt = now;
        if (
          patch.status === undefined &&
          (yield* store.waitingBytes(watch.id)) > COMMAND_WATCH_BACKLOG_LIMIT_BYTES
        ) {
          yield* store.note({
            watchId: watch.id,
            change: commandWatchPauseNotice("storage-limit", "", now),
            now,
          });
          patch.status = "paused";
          patch.pauseReason = "storage-limit";
        }
      }
      if (patch.status === undefined && watch.status !== "watching") patch.status = "watching";
      if (watch.collectionFailures !== 0) patch.collectionFailures = 0;
      if (watch.comparisonFailures !== 0) patch.comparisonFailures = 0;
      if (watch.lastFailure !== undefined) patch.lastFailure = null;
    }
    // The update's own wording follows what this check decided, such as that the watch ended.
    const delivery = yield* planDelivery(thread.id, {
      ...watch,
      ...(patch.status === "ended" ? { status: "ended" as const } : {}),
    });
    const merged = { ...patch, ...delivery.patch };
    if (Object.keys(merged).length > 0 || delivery.wake !== undefined) {
      yield* record(thread.id, watch, merged, delivery.wake);
    }
    return failures;
  });

  const deliverOnly = Effect.fn("CommandWatchReactor.deliverOnly")(function* (
    thread: ProjectionStore.ProjectionThreadCommandWatches,
    watch: ThreadCommandWatch,
  ) {
    const delivery = yield* planDelivery(thread.id, watch);
    if (Object.keys(delivery.patch).length > 0 || delivery.wake !== undefined) {
      yield* record(thread.id, watch, delivery.patch, delivery.wake);
    }
    // Half the limit, so a watch near it does not pause and resume on every check.
    if (
      watch.status === "paused" &&
      watch.pauseReason === "storage-limit" &&
      (yield* store.waitingBytes(watch.id)) <= COMMAND_WATCH_BACKLOG_LIMIT_BYTES / 2
    ) {
      yield* engine.dispatch({
        type: "thread.command-watch.resume",
        commandId: CommandId.make(
          `server:command-watch:${watch.id}:${yield* crypto.randomUUIDv4()}`,
        ),
        threadId: thread.id,
        watchId: watch.id,
      });
    }
  });

  /** Moves a replaced watch's waiting changes to the watch that took over, then delivers them. */
  const adopt = Effect.fn("CommandWatchReactor.adopt")(function* (
    thread: ProjectionStore.ProjectionThreadCommandWatches,
    watch: ThreadCommandWatch,
    replacedId: string,
  ) {
    const { runs: threadRuns } = yield* projections.getThreadRecords(thread.id, ["runs"]);
    // An update of the replaced watch that started before the takeover already reached the agent.
    const prefix = `message:command-watch:${replacedId}:`;
    const deliveredThrough = Math.max(
      0,
      ...threadRuns.flatMap((run) =>
        run.userMessageId.startsWith(prefix) &&
        run.status !== "cancelled" &&
        run.status !== "queued" &&
        !neverReachedAgent(run)
          ? [throughOf(run.userMessageId)]
          : [],
      ),
    );
    yield* store.adopt({
      fromWatchId: replacedId,
      toWatchId: watch.id,
      deliveredThrough,
      now: DateTime.formatIso(yield* DateTime.now),
    });
    const delivery = yield* planDelivery(thread.id, watch);
    yield* record(thread.id, watch, { ...delivery.patch, replacesWatchId: null }, delivery.wake);
  });

  const removeOrphans = Effect.fn("CommandWatchReactor.removeOrphans")(function* (
    present: ReadonlySet<string>,
    now: number,
  ) {
    const ids = yield* store.watchIds;
    for (const id of ids) {
      if (present.has(id)) continue;
      const storage = yield* store.get(id);
      if (storage === undefined) continue;
      if (!orphanSeenAt.has(id)) {
        orphanSeenAt.set(id, now);
        continue;
      }
      if (now - orphanSeenAt.get(id)! < ORPHAN_GRACE_MS) continue;
      orphanSeenAt.delete(id);
      yield* store.remove(id);
    }
  });
  const orphanSeenAt = new Map<string, number>();
  const discarded = new Set<string>();

  const sweep = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    // The scheduler ticks every few seconds and the scan reads every watched thread, so it runs
    // only when a watch or run changed or a backstop passed.
    if (!watchesChanged && now < nextScanAt) return;
    watchesChanged = false;
    const threads = yield* projections.getThreadsWithCommandWatches();
    // Ended watches stay listed but need no looking at, so they leave an idle server idle.
    const anyLive = threads.some(
      (thread) =>
        mayRun(thread) &&
        thread.commandWatches.some(
          (watch) => watch.status !== "ended" || watch.waitingChanges > 0 || watch.pendingMessageId,
        ),
    );
    nextScanAt = now + (anyLive ? ACTIVE_SCAN_MS : IDLE_SCAN_MS);
    // A replaced watch's storage stays until its replacement adopted the waiting changes.
    const present = new Set(
      threads.flatMap((thread) =>
        thread.commandWatches.flatMap((w) =>
          w.replacesWatchId === undefined ? [w.id] : [w.id, w.replacesWatchId],
        ),
      ),
    );
    const presentKeys = new Set(threads.flatMap((thread) => thread.commandWatches.map(watchKey)));
    for (const key of dueAt.keys()) if (!presentKeys.has(key)) dueAt.delete(key);
    yield* removeOrphans(present, now).pipe(
      Effect.catchCause((cause) => Effect.logWarning("command watch cleanup failed", { cause })),
    );
    for (const thread of threads) {
      for (const watch of thread.commandWatches) {
        // A deliberate stop wants nothing more from the watch; a completed one still delivers.
        if (
          watch.status === "ended" &&
          watch.endReason !== "completed" &&
          !discarded.has(watch.id)
        ) {
          discarded.add(watch.id);
          yield* store.discardWaiting(watch.id).pipe(
            Effect.catchCause((cause) => {
              discarded.delete(watch.id);
              return Effect.logWarning("command watch discard failed", { cause });
            }),
          );
        }
      }
      if (!mayRun(thread)) continue;
      for (const watch of thread.commandWatches) {
        const key = watchKey(watch);
        if (yield* FiberMap.has(runs, key)) continue;
        const replacedId = watch.replacesWatchId;
        if (
          replacedId !== undefined &&
          watch.status !== "awaiting_approval" &&
          !thread.commandWatches.some(
            (other) => other.id === replacedId && other.status !== "ended",
          )
        ) {
          yield* FiberMap.run(
            runs,
            key,
            adopt(thread, watch, replacedId).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("command watch takeover failed", {
                  threadId: thread.id,
                  watchId: watch.id,
                  cause,
                }),
              ),
            ),
          );
          continue;
        }
        // Backoff survives a restart; missed intervals are never caught up, only the next one runs.
        if (running(watch) && !dueAt.has(key)) {
          const storage = yield* store.get(watch.id);
          dueAt.set(key, storage?.nextCheckAt ?? 0);
        }
        const due = running(watch) && (dueAt.get(key) ?? 0) <= now;
        const delivering = watch.pendingMessageId !== undefined || watch.waitingChanges > 0;
        if (!due && !delivering) continue;
        const work = due
          ? permits
              .withPermits(1)(check(thread.id, key))
              .pipe(
                Effect.tap((failures) =>
                  Effect.gen(function* () {
                    const finished = yield* Clock.currentTimeMillis;
                    // The interval starts when a check ends, so checks of one watch never overlap.
                    const intervals =
                      failures === undefined || failures === 0
                        ? 1
                        : Math.min(2 ** (failures - 1), MAX_BACKOFF_INTERVALS);
                    const next = finished + watch.everyMinutes * 60_000 * intervals;
                    dueAt.set(key, next);
                    nextScanAt = Math.min(nextScanAt, next);
                    yield* store.setNextCheckAt(watch.id, next);
                  }),
                ),
              )
          : deliverOnly(thread, watch);
        yield* FiberMap.run(
          runs,
          key,
          work.pipe(
            Effect.asVoid,
            Effect.catchCause((cause) =>
              Effect.logWarning("command watch check failed", {
                threadId: thread.id,
                watchId: watch.id,
                cause,
              }).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    if (due) dueAt.set(key, now + watch.everyMinutes * 60_000);
                  }),
                ),
              ),
            ),
          ),
        );
      }
    }
    // A scan on its own schedule would run each check up to a scan late.
    for (const due of dueAt.values()) if (due > now) nextScanAt = Math.min(nextScanAt, due);
  }).pipe(
    Effect.catchCause((cause) => Effect.logWarning("command watch sweep failed", { cause })),
    Effect.withSpan("CommandWatchReactor.sweep"),
  );

  /** Looks again soon after a watch or run changes, and stops a check whose watch ended. */
  const onEvent = (event: OrchestrationV2DomainEvent) =>
    Effect.suspend(() => {
      if (
        event.type === "thread.command-watches-updated" ||
        event.type === "thread.settled" ||
        event.type === "thread.unsettled" ||
        event.type === "run.created" ||
        event.type === "run.updated"
      ) {
        watchesChanged = true;
      }
      if (event.type !== "thread.command-watches-updated") return Effect.void;
      // A completed watch's own check recorded the end, and still has its work to finish.
      const stopped = (event.payload.commandWatches ?? []).filter(
        (watch) => watch.status === "ended" && watch.endReason !== "completed",
      );
      return Effect.forEach(stopped, (watch) => FiberMap.remove(runs, watchKey(watch)), {
        discard: true,
      });
    });

  const start: CommandWatchReactor["Service"]["start"] = Effect.fn("CommandWatchReactor.start")(
    function* () {
      // Losing the stream only delays a stop to the next look, so it is logged and resubscribed.
      yield* forkParked(
        Stream.runForEach(engine.streamDomainEvents, onEvent).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("command watch event stream failed", { cause }),
          ),
          Effect.andThen(Effect.sync(() => (watchesChanged = true))),
          Effect.repeat(Schedule.spaced("5 seconds")),
        ),
      );
    },
  );

  return {
    start,
    sweep,
    awaitRuns: FiberMap.awaitEmpty(runs).pipe(Effect.ignore),
  } satisfies CommandWatchReactor["Service"];
});

export const layer = Layer.effect(CommandWatchReactor, make);

/** Starts the reactor and registers its look for due watches with the shared scheduler. */
export const layerStart = Layer.effectDiscard(
  Effect.gen(function* () {
    const reactor = yield* CommandWatchReactor;
    const scheduler = yield* Scheduler.Scheduler;
    yield* reactor.start();
    yield* scheduler.register("command-watch", reactor.sweep);
  }),
);
