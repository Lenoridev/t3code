// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type RunId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as CommandWatchReactor from "./CommandWatchReactor.ts";
import * as CommandWatchStore from "./CommandWatchStore.ts";
import { COMMAND_WATCH_BACKLOG_LIMIT_BYTES, commandWatchRevisionText } from "./commandWatch.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Runs here never reach a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
// No effect worker: runs stay where the test puts them.
const layerTest = ThreadManagementService.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      layerDatabase,
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      ProjectStore.layer.pipe(Layer.provide(layerDatabase)),
      EventStore.layer.pipe(Layer.provide(layerDatabase)),
      EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
      ProviderReplayHarness.layerWithRegistry(
        { name: "command-watch" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ),
  ),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
  Layer.provideMerge(CommandWatchStore.layer.pipe(Layer.provide(layerDatabase))),
  Layer.provideMerge(NodeServices.layer),
);

const output = (
  overrides: Partial<ProcessRunner.ProcessRunOutput>,
): ProcessRunner.ProcessRunOutput => ({
  stdout: "",
  stderr: "",
  code: 0 as ProcessRunner.ProcessRunOutput["code"],
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
  ...overrides,
});

/** A process runner that answers with `next()` and records each run. */
const fakeRunner = (next: () => ProcessRunner.ProcessRunOutput) => {
  const calls: Array<ProcessRunner.ProcessRunInput> = [];
  const layer = Layer.succeed(ProcessRunner.ProcessRunner, {
    run: (input) =>
      Effect.sync(() => {
        calls.push(input);
        return next();
      }),
  });
  return { calls, layer };
};

const createThread = (
  threadId: ThreadId,
  modes: {
    readonly runtimeMode?: "full-access" | "approval-required";
    readonly interactionMode?: "default" | "plan";
  } = {},
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId: ProjectId.make("project:command-watch"),
      title: threadId,
      modelSelection,
      runtimeMode: modes.runtimeMode ?? "full-access",
      interactionMode: modes.interactionMode ?? "default",
      branch: null,
      worktreePath: process.cwd(),
      createdBy: "user",
      creationSource: "web",
    });
  });

/** Starts a run and marks it running, as an agent calling a tool would be. */
const startRun = (threadId: ThreadId, text: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send:${threadId}:${text}`),
      threadId,
      messageId: MessageId.make(`message:${threadId}:${text}`),
      text,
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    const run = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
    const now = yield* DateTime.now;
    yield* projections.apply({
      id: EventId.make(`event:${threadId}:${text}:running`),
      type: "run.updated",
      threadId,
      runId: run.id,
      occurredAt: now,
      payload: { ...run, status: "running", startedAt: now },
    });
    return run.id;
  });

const finishRuns = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    for (const run of (yield* orchestrator.getThreadProjection(threadId)).runs) {
      if (run.status === "completed") continue;
      yield* projections.apply({
        id: EventId.make(`event:${threadId}:${run.id}:completed`),
        type: "run.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: { ...run, status: "completed", startedAt: now, completedAt: now },
      });
    }
  });

const sha256 = (text: string) => NodeCrypto.createHash("sha256").update(text).digest("hex");

const startWatch = (
  threadId: ThreadId,
  callerRunId: RunId,
  watchId: string,
  command: string,
  options: {
    readonly format?: "text" | "json";
    readonly helpers?: ReadonlyArray<{ readonly path: string; readonly content: string }>;
    /** What storage holds, when it differs from what was hashed. */
    readonly storedHelpers?: ReadonlyArray<{ readonly path: string; readonly content: string }>;
    readonly replaces?: string;
  } = {},
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const store = yield* CommandWatchStore.CommandWatchStore;
    const format = options.format ?? "text";
    const helpers = (options.helpers ?? []).map((helper) => ({
      path: helper.path,
      sha256: sha256(helper.content),
    }));
    const cwd = process.cwd();
    yield* store.create({
      watchId,
      threadId,
      helpers: options.storedHelpers ?? options.helpers ?? [],
      now: "2026-10-30T00:00:00.000Z",
    });
    return yield* orchestrator.dispatch({
      type: "thread.command-watch.start",
      commandId: CommandId.make(`watch:${threadId}:${watchId}:${command}`),
      threadId,
      callerRunId,
      watchId,
      command,
      label: `label ${command}`,
      everyMinutes: 10,
      format,
      helpers,
      reportInitialState: false,
      cwd,
      revisionHash: sha256(commandWatchRevisionText({ command, format, cwd, helpers })),
      ...(options.replaces === undefined ? {} : { replacesWatchId: options.replaces }),
    });
  });

const watchOf = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    return projection.thread.commandWatches?.[0];
  });

const watchesOf = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    return (projection.thread.commandWatches ?? []).map((watch) => watch.id);
  });

const updatesOf = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    return projection.messages.filter((message) => message.notification?.source.kind === "monitor");
  });

/** The agent starts on whatever waits in the queue. */
const takeQueued = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    for (const run of (yield* orchestrator.getThreadProjection(threadId)).runs) {
      if (run.status !== "queued" && run.status !== "preparing") continue;
      yield* projections.apply({
        id: EventId.make(`event:${threadId}:${run.id}:taken`),
        type: "run.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: { ...run, status: "running", startedAt: now },
      });
    }
  });

it.effect(
  "runs at once in Full access and waits for approval of the exact revision otherwise",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const full = ThreadId.make("thread:command-watch-full");
      yield* createThread(full);
      yield* startWatch(full, yield* startRun(full, "work"), "command-watch:full", "echo a");
      const started = yield* watchOf(full);
      assert.strictEqual(started?.status, "baselining");
      assert.deepEqual(started?.authority, { kind: "full-access" });

      const supervised = ThreadId.make("thread:command-watch-supervised");
      yield* createThread(supervised, { runtimeMode: "approval-required" });
      yield* startWatch(
        supervised,
        yield* startRun(supervised, "work"),
        "command-watch:a",
        "echo a",
      );
      const pending = (yield* watchOf(supervised))!;
      assert.strictEqual(pending.status, "awaiting_approval");
      assert.isNull(pending.authority);

      const runner = fakeRunner(() => output({ stdout: "a\n" }));
      yield* Effect.gen(function* () {
        const reactor = yield* CommandWatchReactor.make;
        const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
        yield* look;
        // Only the Full access watch ran.
        assert.lengthOf(runner.calls, 1);

        const approve = (revisionHash: string, commandId: string) =>
          orchestrator.dispatch({
            type: "thread.command-watch.approve",
            commandId: CommandId.make(commandId),
            threadId: supervised,
            watchId: pending.id,
            revisionHash,
            approve: true,
          });
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(approve("other-revision", "approve-stale"))),
        );
        yield* approve(pending.revisionHash, "approve");
        // No event stream runs here, so the next look waits out the scan interval.
        yield* TestClock.adjust("1 minute");
        const approved = yield* watchOf(supervised);
        assert.strictEqual(approved?.status, "baselining");
        assert.deepEqual(approved?.authority, {
          kind: "approved",
          revisionHash: pending.revisionHash,
        });
        yield* look;
        assert.lengthOf(runner.calls, 2);
        assert.strictEqual((yield* watchOf(supervised))?.status, "watching");
      }).pipe(Effect.provide(runner.layer), Effect.scoped);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "records a quiet baseline, queues one update at a time, and keeps checking while busy",
  () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread:command-watch-delivery");
      yield* createThread(threadId);
      yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");
      // The agent stays busy, so updates wait in the queue behind its run.

      let next = output({ stdout: "comment 1\n" });
      const runner = fakeRunner(() => next);
      yield* Effect.gen(function* () {
        const reactor = yield* CommandWatchReactor.make;
        const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));

        yield* look;
        assert.lengthOf(yield* updatesOf(threadId), 0);
        assert.strictEqual((yield* watchOf(threadId))?.status, "watching");

        // Not due again until its interval passes.
        yield* look;
        assert.lengthOf(runner.calls, 1);

        next = output({ stdout: "comment 1\ncomment 2\n" });
        yield* TestClock.adjust("10 minutes");
        yield* look;
        const first = yield* updatesOf(threadId);
        assert.lengthOf(first, 1);
        assert.include(first[0]?.text ?? "", "+ comment 2");
        assert.isDefined((yield* watchOf(threadId))?.pendingMessageId);

        // The first update still waits, so a second change waits behind it instead.
        next = output({ stdout: "comment 1\ncomment 2\ncomment 3\n" });
        yield* TestClock.adjust("10 minutes");
        yield* look;
        assert.lengthOf(runner.calls, 3);
        assert.lengthOf(yield* updatesOf(threadId), 1);

        // Once the agent starts on it, the waiting change goes out, and checks keep running.
        yield* takeQueued(threadId);
        yield* TestClock.adjust("1 minute");
        yield* look;
        const second = yield* updatesOf(threadId);
        assert.lengthOf(second, 2);
        assert.include(second[1]?.text ?? "", "+ comment 3");
        next = output({ stdout: "comment 1\ncomment 2\ncomment 3\ncomment 4\n" });
        yield* TestClock.adjust("10 minutes");
        yield* look;
        assert.lengthOf(runner.calls, 4);
        assert.lengthOf(yield* updatesOf(threadId), 2);
      }).pipe(Effect.provide(runner.layer), Effect.scoped);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("pauses after repeated failures with a notice and resumes from the accepted output", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:command-watch-failures");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");
    yield* finishRuns(threadId);

    let next = output({ stdout: "open\n" });
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;

      // Exit 1 without stderr is a failure too, not a quiet check.
      next = output({ code: 1 as ProcessRunner.ProcessRunOutput["code"] });
      for (let attempt = 0; attempt < 8; attempt++) {
        yield* TestClock.adjust("1 day");
        yield* look;
      }
      const paused = yield* watchOf(threadId);
      assert.strictEqual(paused?.status, "paused");
      assert.strictEqual(paused?.pauseReason, "failures");
      assert.include((yield* updatesOf(threadId)).at(-1)?.text ?? "", "paused this watch");
      yield* TestClock.adjust("1 day");
      yield* look;
      assert.lengthOf(runner.calls, 9);

      yield* orchestrator.dispatch({
        type: "thread.command-watch.resume",
        commandId: CommandId.make("resume"),
        threadId,
        watchId: paused!.id,
      });
      assert.strictEqual((yield* watchOf(threadId))?.collectionFailures, 0);
      next = output({ stdout: "closed\n" });
      yield* takeQueued(threadId);
      yield* TestClock.adjust("1 day");
      yield* look;
      assert.lengthOf(runner.calls, 10);
      assert.include((yield* updatesOf(threadId)).at(-1)?.text ?? "", "+ closed");
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("pauses at the storage limit and resumes once the backlog drains", () =>
  Effect.gen(function* () {
    const store = yield* CommandWatchStore.CommandWatchStore;
    const threadId = ThreadId.make("thread:command-watch-storage");
    yield* createThread(threadId);
    // The agent stays busy, so changes pile up behind its run.
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");

    let next = output({ stdout: "a\n" });
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      yield* store.accept({
        watchId: "command-watch:a",
        observation: "a\n",
        changes: [
          {
            kind: "output.changed",
            summary: "large change",
            content: "x".repeat(COMMAND_WATCH_BACKLOG_LIMIT_BYTES),
          },
        ],
        now: DateTime.formatIso(yield* DateTime.now),
      });
      next = output({ stdout: "b\n" });
      yield* TestClock.adjust("10 minutes");
      yield* look;
      const paused = yield* watchOf(threadId);
      assert.strictEqual(paused?.status, "paused");
      assert.strictEqual(paused?.pauseReason, "storage-limit");

      // The update carrying the large change is taken, which frees the storage.
      yield* takeQueued(threadId);
      yield* TestClock.adjust("1 minute");
      yield* look;
      const resumed = yield* watchOf(threadId);
      assert.strictEqual(resumed?.status, "watching");
      assert.isUndefined(resumed?.pauseReason);
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a replacement takes over the changes the replaced watch had not delivered", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:command-watch-replace");
    yield* createThread(threadId);
    // The agent stays busy, so the old watch's changes wait behind its run.
    const runId = yield* startRun(threadId, "work");
    yield* startWatch(threadId, runId, "command-watch:old", "gh api");

    let next = output({ stdout: "a\n" });
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      next = output({ stdout: "a\nb\n" });
      yield* TestClock.adjust("10 minutes");
      yield* look;
      next = output({ stdout: "a\nb\nc\n" });
      yield* TestClock.adjust("10 minutes");
      yield* look;

      // Full access, so the replacement takes over at once.
      yield* startWatch(threadId, runId, "command-watch:new", "gh api --paginate", {
        replaces: "command-watch:old",
      });
      assert.deepEqual(yield* watchesOf(threadId), ["command-watch:new"]);
      yield* TestClock.adjust("1 minute");
      yield* look;
      const taken = yield* watchOf(threadId);
      assert.isUndefined(taken?.replacesWatchId);
      const update = (yield* updatesOf(threadId)).at(-1)?.text ?? "";
      assert.include(update, "+ b");
      assert.include(update, "+ c");
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("Stop cancels the update the agent has not started on", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:command-watch-stop");
    yield* createThread(threadId);
    // The agent stays busy, so the update queues behind its run.
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");

    let next = output({ stdout: "a\n" });
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      next = output({ stdout: "b\n" });
      yield* TestClock.adjust("10 minutes");
      yield* look;
      const update = (yield* updatesOf(threadId))[0]!;
      const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === update.id,
      );
      assert.strictEqual(queued?.status, "queued");

      yield* orchestrator.dispatch({
        type: "thread.command-watch.stop",
        commandId: CommandId.make("stop"),
        threadId,
        watchId: "command-watch:a",
      });
      assert.deepEqual(yield* watchesOf(threadId), []);
      const cancelled = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.id === queued?.id,
      );
      assert.strictEqual(cancelled?.status, "cancelled");
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("leaving Full access, or a changed helper copy, sends a watch back for approval", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const moved = ThreadId.make("thread:command-watch-mode");
    yield* createThread(moved);
    yield* startWatch(moved, yield* startRun(moved, "work"), "command-watch:mode", "echo a");
    yield* orchestrator.dispatch({
      type: "thread.runtime-mode.set",
      commandId: CommandId.make("leave-full-access"),
      threadId: moved,
      runtimeMode: "approval-required",
    });
    const waiting = yield* watchOf(moved);
    assert.strictEqual(waiting?.status, "awaiting_approval");
    assert.isNull(waiting?.authority);

    const tampered = ThreadId.make("thread:command-watch-tampered");
    yield* createThread(tampered, { runtimeMode: "approval-required" });
    yield* startWatch(tampered, yield* startRun(tampered, "work"), "command-watch:t", "sh x.sh", {
      helpers: [{ path: "x.sh", content: "echo approved" }],
      storedHelpers: [{ path: "x.sh", content: "echo changed" }],
    });
    const pending = (yield* watchOf(tampered))!;
    yield* orchestrator.dispatch({
      type: "thread.command-watch.approve",
      commandId: CommandId.make("approve-tampered"),
      threadId: tampered,
      watchId: pending.id,
      revisionHash: pending.revisionHash,
      approve: true,
    });

    const runner = fakeRunner(() => output({ stdout: "a\n" }));
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      yield* reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      assert.lengthOf(runner.calls, 0);
      assert.strictEqual((yield* watchOf(tampered))?.status, "awaiting_approval");
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("limits watches per thread and ends them when the thread settles or archives", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:command-watch-limit");
    yield* createThread(threadId);
    const runId = yield* startRun(threadId, "work");
    for (const name of ["a", "b", "c", "d", "e"]) {
      yield* startWatch(threadId, runId, `command-watch:${name}`, `echo ${name}`);
    }
    // An identical definition is the existing watch, not a sixth one.
    assert.lengthOf(
      (yield* startWatch(threadId, runId, "command-watch:dup", "echo a")).storedEvents,
      0,
    );
    assert.isTrue(
      Exit.isFailure(yield* Effect.exit(startWatch(threadId, runId, "command-watch:f", "echo f"))),
    );
    yield* finishRuns(threadId);
    yield* orchestrator.dispatch({
      type: "thread.archive",
      commandId: CommandId.make("archive"),
      threadId,
    });
    assert.deepEqual(yield* watchesOf(threadId), []);
  }).pipe(Effect.provide(layerTest)),
);
