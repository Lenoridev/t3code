import {
  CommandId,
  MessageId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Notification,
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

import * as ProcessRunner from "../processRunner.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { forkParked } from "../serverActivation.ts";
import {
  COMMAND_WATCH_BACKLOG_LIMIT_BYTES,
  COMMAND_WATCH_FAILURE_LIMIT,
  COMMAND_WATCH_OUTPUT_LIMIT_BYTES,
  type CommandWatchRun,
  commandWatchPauseNotice,
  commandWatchRevisionText,
  commandWatchUpdate,
  evaluateCommandWatchCheck,
} from "./commandWatch.ts";
import * as CommandWatchStore from "./CommandWatchStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

/** Checks at once across the environment. */
const CONCURRENT_RUNS = 4;
const RUN_TIMEOUT_SECONDS = 60;
/** How often watched threads are looked up while any exist, and when none do. */
const ACTIVE_SCAN_MS = 30_000;
const IDLE_SCAN_MS = 5 * 60_000;
/** Captured beyond the output limit so an over-long result reads as too long, not as cut. */
const CAPTURE_LIMIT_BYTES = COMMAND_WATCH_OUTPUT_LIMIT_BYTES * 2;
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

/** The update message carries the last change it covers, so a restart can tell what it held. */
const updateMessageId = (watch: ThreadCommandWatch, through: number) =>
  MessageId.make(`message:command-watch:${watch.id}:${through}`);
const throughOf = (messageId: string) => Number(messageId.slice(messageId.lastIndexOf(":") + 1));

/**
 * The server's own shell, run non-interactively: the server already loaded the login PATH.
 * Resolved once, since finding the user's shell can read the account database.
 */
const resolveShell = Effect.gen(function* () {
  const platform = yield* HostProcess.Platform;
  if (platform === "win32") {
    return (command: string) => ({
      command: "powershell.exe",
      args: ["-NoProfile", "-NonInteractive", "-Command", command],
    });
  }
  const environment = yield* HostProcess.Environment;
  const shell = listLoginShellCandidates(platform, environment.SHELL)[0] ?? "/bin/sh";
  return (command: string) => ({ command: shell, args: ["-c", command] });
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
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const permits = yield* Semaphore.make(CONCURRENT_RUNS);
  const shellInvocation = yield* resolveShell;
  const runs = yield* FiberMap.make<string, void, never>();
  const dueAt = new Map<string, number>();
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
    const suffix = wake === undefined ? yield* crypto.randomUUIDv4() : `update:${wake.messageId}`;
    yield* engine.dispatch({
      type: "thread.command-watch.sync",
      // An update's command id follows its message, so a retry after a crash is not queued twice.
      commandId: CommandId.make(`server:command-watch:${watch.id}:${suffix}`),
      threadId,
      watchId: watch.id,
      startedAt: watch.startedAt,
      patch,
      ...(wake === undefined ? {} : { wake }),
    });
  });

  /**
   * Notices when the agent took the queued update, then queues the next one from waiting changes.
   * Returns the patch and update to record; changes count as delivered once their update starts.
   */
  const planDelivery = Effect.fn("CommandWatchReactor.planDelivery")(function* (
    threadId: ThreadId,
    watch: ThreadCommandWatch,
  ) {
    const patch: { -readonly [K in keyof SyncPatch]: SyncPatch[K] } = {};
    if (watch.pendingMessageId !== undefined) {
      const { runs: threadRuns } = yield* projections.getThreadRecords(threadId, ["runs"]);
      const run = threadRuns.find(
        (candidate) => candidate.userMessageId === watch.pendingMessageId,
      );
      if (run !== undefined && (run.status === "queued" || run.status === "preparing")) {
        return { patch, wake: undefined };
      }
      // Started, finished, or removed from the queue by the user: either way it left the queue.
      yield* store.markDelivered(watch.id, throughOf(watch.pendingMessageId));
      patch.pendingMessageId = null;
    }
    const update = commandWatchUpdate(watch, yield* store.waiting(watch.id));
    if (update === undefined) {
      if (watch.waitingChanges !== 0) patch.waitingChanges = 0;
      return { patch, wake: undefined };
    }
    const messageId = updateMessageId(watch, update.through);
    patch.pendingMessageId = messageId;
    patch.waitingChanges = update.remaining;
    return {
      patch,
      wake: { messageId, text: update.text, notification: update.notification } satisfies Wake,
    };
  });

  /** Whether the watch's approval still covers what it would run. */
  const authorized = Effect.fn("CommandWatchReactor.authorized")(function* (
    thread: ProjectionStore.ProjectionThreadCommandWatches,
    watch: ThreadCommandWatch,
    helpers: ReadonlyArray<CommandWatchStore.CommandWatchHelperFile>,
  ) {
    if (watch.authority === null) return false;
    if (watch.authority.kind === "full-access") return thread.runtimeMode === "full-access";
    const helperHashes = yield* Effect.forEach(helpers, (helper) =>
      sha256(helper.content).pipe(Effect.map((sha) => ({ path: helper.path, sha256: sha }))),
    );
    const stored = yield* sha256(
      commandWatchRevisionText({
        command: watch.command,
        format: watch.format,
        cwd: watch.cwd,
        helpers: helperHashes,
      }),
    );
    return stored === watch.revisionHash && watch.authority.revisionHash === watch.revisionHash;
  });

  const execute = Effect.fn("CommandWatchReactor.execute")(function* (
    watch: ThreadCommandWatch,
    helpers: ReadonlyArray<CommandWatchStore.CommandWatchHelperFile>,
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
    const shell = shellInvocation(watch.command);
    return yield* processes
      .run({
        command: shell.command,
        args: shell.args,
        cwd: watch.cwd,
        env: { T3_WATCH_HELPERS: helperDirectory },
        timeout: `${RUN_TIMEOUT_SECONDS} seconds`,
        timeoutBehavior: "timedOutResult",
        maxOutputBytes: CAPTURE_LIMIT_BYTES,
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

  /** One check: run, compare, store what was accepted, then record and deliver in one command. */
  const check = Effect.fn("CommandWatchReactor.check")(function* (
    thread: ProjectionStore.ProjectionThreadCommandWatches,
    watch: ThreadCommandWatch,
  ) {
    const now = DateTime.formatIso(yield* DateTime.now);
    let storage = yield* store.get(watch.id);
    if (storage === undefined) {
      yield* store.create({ watchId: watch.id, threadId: thread.id, helpers: [], now });
      storage = { helpers: [], observation: null, deliveredThrough: 0 };
    }
    if (!(yield* authorized(thread, watch, storage.helpers))) {
      // A changed definition or helper never runs under an approval given for another one.
      yield* record(thread.id, watch, { status: "awaiting_approval" });
      return;
    }
    const outcome = evaluateCommandWatchCheck({
      format: watch.format,
      previous: storage.observation,
      reportInitialState: watch.reportInitialState,
      run: yield* execute(watch, storage.helpers),
    });
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
      }
    } else {
      if (outcome.kind === "accepted") {
        yield* store.accept({
          watchId: watch.id,
          observation: outcome.observation,
          changes: outcome.changes,
          now,
        });
        if (outcome.changes.length > 0) patch.lastChangeAt = now;
        if ((yield* store.waitingBytes(watch.id)) > COMMAND_WATCH_BACKLOG_LIMIT_BYTES) {
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
    const delivery = yield* planDelivery(thread.id, watch);
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
        run.status !== "queued"
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

  const sweep = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    // The scheduler ticks every few seconds and the scan reads every watched thread, so it runs
    // only when a watch or run changed or a backstop passed.
    if (!watchesChanged && now < nextScanAt) return;
    watchesChanged = false;
    const threads = yield* projections.getThreadsWithCommandWatches();
    nextScanAt = now + (threads.length > 0 ? ACTIVE_SCAN_MS : IDLE_SCAN_MS);
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
      if (!mayRun(thread)) continue;
      for (const watch of thread.commandWatches) {
        const key = watchKey(watch);
        if (yield* FiberMap.has(runs, key)) continue;
        const replacedId = watch.replacesWatchId;
        if (
          replacedId !== undefined &&
          watch.status !== "awaiting_approval" &&
          !thread.commandWatches.some((other) => other.id === replacedId)
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
        const due = running(watch) && (dueAt.get(key) ?? 0) <= now;
        const delivering = watch.pendingMessageId !== undefined || watch.waitingChanges > 0;
        if (!due && !delivering) continue;
        const work = due
          ? permits
              .withPermits(1)(check(thread, watch))
              .pipe(
                Effect.tap((failures) =>
                  Clock.currentTimeMillis.pipe(
                    Effect.map((finished) => {
                      // The interval starts when a check ends, so checks of one watch never overlap.
                      const intervals =
                        failures === undefined || failures === 0
                          ? 1
                          : Math.min(2 ** (failures - 1), MAX_BACKOFF_INTERVALS);
                      dueAt.set(key, finished + watch.everyMinutes * 60_000 * intervals);
                    }),
                  ),
                ),
              )
          : deliverOnly(thread, watch);
        if (due) dueAt.set(key, Number.POSITIVE_INFINITY);
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
  }).pipe(
    Effect.catchCause((cause) => Effect.logWarning("command watch sweep failed", { cause })),
    Effect.withSpan("CommandWatchReactor.sweep"),
  );

  /** Looks again soon after a watch or run changes, and stops a check whose watch ended. */
  const onEvent = (event: OrchestrationV2DomainEvent) =>
    Effect.suspend(() => {
      if (
        event.type === "thread.command-watches-updated" ||
        event.type === "run.created" ||
        event.type === "run.updated"
      ) {
        watchesChanged = true;
      }
      if (event.type !== "thread.command-watches-updated") return Effect.void;
      const live = new Set((event.payload.commandWatches ?? []).map(watchKey));
      return Effect.forEach(
        [...dueAt.keys()].filter((key) => !live.has(key) && key.startsWith("command-watch:")),
        (key) => FiberMap.remove(runs, key),
        { discard: true },
      );
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
