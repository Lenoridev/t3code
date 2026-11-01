// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type RunId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as CommandWatchComparison from "./CommandWatchComparison.ts";
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

const TEST_ENVIRONMENT = "environment:test";
const TEST_SHELL = "/bin/sh";

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
  Layer.provideMerge(
    // The interpreter has its own tests; here a comparison only has to run on its input.
    Layer.succeed(CommandWatchComparison.CommandWatchComparison, {
      compile: () => Effect.void,
      compare: (source, input) =>
        Effect.try({
          try: () =>
            JSON.stringify(
              (new Function(`${source}; return compare;`)() as (i: unknown) => unknown)(input),
            ),
          catch: (cause) =>
            new CommandWatchComparison.CommandWatchComparisonError({
              reason: `threw: ${String(cause)}`,
              unavailable: false,
            }),
        }),
    }),
  ),
  Layer.provideMerge(
    Layer.succeed(ServerEnvironment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make(TEST_ENVIRONMENT)),
    } as never),
  ),
  Layer.provideMerge(Layer.succeed(HostProcess.Environment, { SHELL: "/bin/sh" })),
  Layer.provideMerge(Layer.succeed(HostProcess.Platform, "linux")),
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
    readonly requestId?: string;
    readonly comparisonSource?: string;
    readonly completionCondition?: string;
    /** The environment the watch was approved in, when it differs from this one. */
    readonly environmentId?: string;
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
      comparisonSource: options.comparisonSource,
      now: "2026-10-30T00:00:00.000Z",
    });
    const sourceHash =
      options.comparisonSource === undefined ? null : sha256(options.comparisonSource);
    const completion =
      options.completionCondition === undefined
        ? ({ kind: "ongoing" } as const)
        : ({ kind: "condition", description: options.completionCondition } as const);
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
      shell: TEST_SHELL,
      comparison:
        sourceHash === null
          ? { kind: format === "json" ? "json-v1" : "text-v1" }
          : { kind: "custom-js-v1", sha256: sourceHash },
      completion,
      clientRequestId: options.requestId ?? `request:${watchId}`,
      revisionHash: sha256(
        commandWatchRevisionText({
          environmentId: options.environmentId ?? TEST_ENVIRONMENT,
          threadId,
          command,
          format,
          cwd,
          shell: TEST_SHELL,
          comparisonSourceSha256: sourceHash,
          completion,
          everyMinutes: 10,
          reportInitialState: false,
          helpers,
        }),
      ),
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
      // A pause ends a snooze, so snoozing now could only wake the thread straight away.
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            orchestrator.dispatch({
              type: "thread.snooze",
              commandId: CommandId.make("snooze:paused"),
              threadId,
              snoozedUntil: "2099-01-01T00:00:00.000Z",
            }),
          ),
        ),
      );
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
      assert.strictEqual((yield* watchOf(threadId))?.endReason, "stopped");
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

it.effect("limits watches per thread and ends them when the thread archives", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:command-watch-limit");
    yield* createThread(threadId);
    const runId = yield* startRun(threadId, "work");
    for (const name of ["a", "b", "c", "d", "e"]) {
      yield* startWatch(threadId, runId, `command-watch:${name}`, `echo ${name}`);
    }
    // A retry of a request is the existing watch, not a sixth one.
    assert.lengthOf(
      (yield* startWatch(threadId, runId, "command-watch:retry", "echo a", {
        requestId: "request:command-watch:a",
      })).storedEvents,
      0,
    );
    // The same text under a new request is a new watch, so this one is over the limit.
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(startWatch(threadId, runId, "command-watch:again", "echo a")),
      ),
    );
    // The same request id for something else is a mistake.
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          startWatch(threadId, runId, "command-watch:conflict", "echo other", {
            requestId: "request:command-watch:a",
          }),
        ),
      ),
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
    const ended = (yield* orchestrator.getThreadProjection(threadId)).thread.commandWatches ?? [];
    assert.lengthOf(ended, 5);
    assert.isTrue(ended.every((watch) => watch.status === "ended" && watch.authority === null));
  }).pipe(Effect.provide(layerTest)),
);

const COMMENT_COMPARISON = `
  function compare({ phase, previous, current, state }) {
    if (phase === "baseline") return { state: null, changes: [] };
    const seen = new Set(previous.comments.map((comment) => comment.id));
    const changes = current.comments
      .filter((comment) => !seen.has(comment.id) && comment.author !== current.viewer)
      .map((comment) => ({
        key: "comment:" + comment.id,
        kind: "comment.added",
        summary: "new comment",
        details: comment,
      }));
    if (current.state === "BROKEN") throw new Error("Unexpected issue state");
    return {
      state: { checks: (state ? state.checks : 0) + 1 },
      changes,
      ...(current.state === "CLOSED" ? { complete: { reason: "issue closed" } } : {}),
    };
  }
`;

const issue = (state: string, ...comments: ReadonlyArray<readonly [string, string]>) =>
  output({
    stdout: JSON.stringify({
      viewer: "me",
      state,
      comments: comments.map(([id, author]) => ({ id, author })),
    }),
  });

it.effect("a custom comparison decides what is news, and a broken one keeps the baseline", () =>
  Effect.gen(function* () {
    const store = yield* CommandWatchStore.CommandWatchStore;
    const threadId = ThreadId.make("thread:command-watch-custom");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api", {
      format: "json",
      comparisonSource: COMMENT_COMPARISON,
    });
    yield* finishRuns(threadId);

    let next = issue("OPEN");
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      assert.strictEqual((yield* watchOf(threadId))?.status, "watching");
      assert.lengthOf(yield* updatesOf(threadId), 0);

      // The user's own comment changes the output, advances state, and wakes nobody.
      next = issue("OPEN", ["c1", "me"]);
      yield* TestClock.adjust("10 minutes");
      yield* look;
      assert.lengthOf(yield* updatesOf(threadId), 0);
      const quiet = yield* store.get("command-watch:a");
      assert.include(quiet?.observation ?? "", "c1");
      assert.strictEqual(quiet?.comparisonState, '{"checks":1}');

      next = issue("OPEN", ["c1", "me"], ["c2", "other"]);
      yield* TestClock.adjust("10 minutes");
      yield* look;
      const updates = yield* updatesOf(threadId);
      assert.lengthOf(updates, 1);
      assert.strictEqual(updates[0]?.notification?.summary, "label gh api: new comment");
      assert.include(updates[0]?.text ?? "", 'kind "comment.added"');

      // A throwing comparison keeps what was accepted, and a working command does not reset its
      // failure count.
      next = issue("BROKEN", ["c1", "me"], ["c2", "other"], ["c3", "other"]);
      yield* TestClock.adjust("10 minutes");
      yield* look;
      next = output({ code: 1 as ProcessRunner.ProcessRunOutput["code"] });
      yield* TestClock.adjust("1 day");
      yield* look;
      next = issue("BROKEN", ["c1", "me"], ["c2", "other"], ["c3", "other"]);
      yield* TestClock.adjust("1 day");
      yield* look;
      const failing = yield* watchOf(threadId);
      assert.strictEqual(failing?.comparisonFailures, 2);
      assert.strictEqual(failing?.collectionFailures, 0);
      assert.include(failing?.lastFailure ?? "", "Unexpected issue state");
      assert.notInclude((yield* store.get("command-watch:a"))?.observation ?? "", "c3");
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a completing comparison ends the watch and still delivers what it found", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:command-watch-complete");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api", {
      format: "json",
      comparisonSource: COMMENT_COMPARISON,
      completionCondition: "the issue closes",
    });
    yield* finishRuns(threadId);

    let next = issue("OPEN");
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      next = issue("CLOSED", ["c1", "other"]);
      yield* TestClock.adjust("10 minutes");
      yield* look;

      const ended = yield* watchOf(threadId);
      assert.strictEqual(ended?.status, "ended");
      assert.strictEqual(ended?.endReason, "completed");
      assert.strictEqual(ended?.endDetail, "issue closed");
      const [update] = yield* updatesOf(threadId);
      assert.strictEqual(update?.notification?.outcome, "completed");
      assert.strictEqual(update?.notification?.summary, "label gh api: 2 changes");
      assert.include(update?.text ?? "", "This watch has ended");

      // It never runs again.
      yield* TestClock.adjust("1 day");
      yield* look;
      assert.lengthOf(runner.calls, 2);
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a comparison on an ongoing watch cannot complete it", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:command-watch-no-complete");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api", {
      format: "json",
      comparisonSource: COMMENT_COMPARISON,
    });
    yield* finishRuns(threadId);

    let next = issue("OPEN");
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      next = issue("CLOSED");
      yield* TestClock.adjust("10 minutes");
      yield* look;
      const watch = yield* watchOf(threadId);
      assert.strictEqual(watch?.status, "watching");
      assert.strictEqual(watch?.comparisonFailures, 1);
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a grant from another environment does not run here, and keeps the saved state", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:command-watch-copied");
    yield* createThread(threadId, { runtimeMode: "approval-required" });
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api", {
      environmentId: "environment:elsewhere",
    });
    yield* finishRuns(threadId);
    const revisionHash = (yield* watchOf(threadId))?.revisionHash ?? "";
    yield* orchestrator.dispatch({
      type: "thread.command-watch.approve",
      commandId: CommandId.make("approve:copied"),
      threadId,
      watchId: "command-watch:a",
      revisionHash,
      approve: true,
    });

    const runner = fakeRunner(() => output({ stdout: "open\n" }));
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      yield* reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      assert.lengthOf(runner.calls, 0);
      const watch = yield* watchOf(threadId);
      assert.strictEqual(watch?.status, "awaiting_approval");
      assert.isNull(watch?.authority);
      assert.notStrictEqual(watch?.revisionHash, revisionHash);
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "stopping keeps the watch visible as ended, discards waiting changes and stays stopped",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const store = yield* CommandWatchStore.CommandWatchStore;
      const threadId = ThreadId.make("thread:command-watch-stop-ended");
      yield* createThread(threadId);
      yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");
      yield* finishRuns(threadId);

      let next = output({ stdout: "one\n" });
      const runner = fakeRunner(() => next);
      yield* Effect.gen(function* () {
        const reactor = yield* CommandWatchReactor.make;
        const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
        yield* look;
        next = output({ stdout: "two\n" });
        yield* TestClock.adjust("10 minutes");
        yield* look;
        next = output({ stdout: "three\n" });
        yield* TestClock.adjust("10 minutes");
        yield* look;

        const stop = (suffix: string) =>
          orchestrator.dispatch({
            type: "thread.command-watch.stop",
            commandId: CommandId.make(`stop:${suffix}`),
            threadId,
            watchId: "command-watch:a",
          });
        yield* stop("first");
        const ended = yield* watchOf(threadId);
        assert.strictEqual(ended?.status, "ended");
        assert.strictEqual(ended?.endReason, "stopped");
        assert.strictEqual(ended?.waitingChanges, 0);
        assert.isUndefined(ended?.pendingMessageId);
        assert.lengthOf((yield* stop("again")).storedEvents, 0);

        yield* TestClock.adjust("1 day");
        yield* look;
        assert.lengthOf(runner.calls, 3);
        assert.lengthOf(yield* store.waiting("command-watch:a"), 0);
      }).pipe(Effect.provide(runner.layer), Effect.scoped);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("a failed check's delay survives a restart, and a paused watch frees its slot", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:command-watch-due");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");
    yield* finishRuns(threadId);

    const runner = fakeRunner(() => output({ code: 1 as ProcessRunner.ProcessRunOutput["code"] }));
    yield* Effect.gen(function* () {
      const first = yield* CommandWatchReactor.make;
      yield* first.sweep.pipe(Effect.andThen(first.awaitRuns));
      assert.lengthOf(runner.calls, 1);
      // A new reactor is what a restart builds, and it must not check again before the delay.
      const second = yield* CommandWatchReactor.make;
      yield* second.sweep.pipe(Effect.andThen(second.awaitRuns));
      assert.lengthOf(runner.calls, 1);
      yield* TestClock.adjust("10 minutes");
      yield* second.sweep.pipe(Effect.andThen(second.awaitRuns));
      assert.lengthOf(runner.calls, 2);
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a stop interrupts only that watch's check, not other threads' checks", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const ids = ["a", "b", "c"].map((name) => ThreadId.make(`thread:command-watch-${name}`));
    for (const [index, threadId] of ids.entries()) {
      yield* createThread(threadId);
      yield* startWatch(
        threadId,
        yield* startRun(threadId, "work"),
        `command-watch:${index}`,
        `cmd-${index}`,
      );
      yield* finishRuns(threadId);
    }
    let otherInterrupted = false;
    const stoppedInterrupted = yield* Deferred.make<void>();
    const blocked = yield* Deferred.make<void>();
    const waiting = yield* Ref.make(0);
    const layer = Layer.succeed(ProcessRunner.ProcessRunner, {
      run: (input) => {
        const command = input.args?.join(" ") ?? "";
        if (command.includes("cmd-0")) return Effect.succeed(output({ stdout: "one\n" }));
        return Ref.updateAndGet(waiting, (count) => count + 1).pipe(
          Effect.tap((count) => (count === 2 ? Deferred.succeed(blocked, undefined) : Effect.void)),
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() =>
            command.includes("cmd-2")
              ? Deferred.succeed(stoppedInterrupted, undefined)
              : Effect.sync(() => {
                  otherInterrupted = true;
                }),
          ),
        );
      },
    });
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      yield* reactor.start();
      yield* Effect.yieldNow;
      yield* reactor.sweep;
      yield* Deferred.await(blocked);
      yield* orchestrator.dispatch({
        type: "thread.command-watch.stop",
        commandId: CommandId.make("stop:other-thread"),
        threadId: ids[0]!,
      });
      yield* orchestrator.dispatch({
        type: "thread.command-watch.stop",
        commandId: CommandId.make("stop:own-thread"),
        threadId: ids[2]!,
      });
      // Events arrive in order, so the first stop was handled once the second one took effect.
      yield* Deferred.await(stoppedInterrupted);
      assert.isFalse(otherInterrupted);
    }).pipe(Effect.provide(layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a check runs when its interval ends, not at the next scan after it", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:command-watch-on-time");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");
    yield* finishRuns(threadId);
    const runner = fakeRunner(() => output({ stdout: "one\n" }));
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      assert.lengthOf(runner.calls, 1);
      const everyMs = ((yield* watchOf(threadId))?.everyMinutes ?? 0) * 60_000;
      // A scan between checks, so the next scan falls just after the check is due.
      yield* TestClock.adjust(`${everyMs - 29_000} millis`);
      yield* look;
      assert.lengthOf(runner.calls, 1);
      yield* TestClock.adjust("29 seconds");
      yield* look;
      assert.lengthOf(runner.calls, 2);
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a paused watch stops checking and resume reports the gap as one change", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:command-watch-agent-pause");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");
    yield* finishRuns(threadId);

    let next = output({ stdout: "one\n" });
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      assert.lengthOf(runner.calls, 1);

      const pause = (suffix: string) =>
        orchestrator.dispatch({
          type: "thread.command-watch.pause",
          commandId: CommandId.make(`pause:${suffix}`),
          threadId,
          watchId: "command-watch:a",
          pausedBy: "agent",
        });
      yield* pause("first");
      yield* pause("again");
      const paused = yield* watchOf(threadId);
      assert.strictEqual(paused?.status, "paused");
      assert.strictEqual(paused?.pauseReason, "agent");

      next = output({ stdout: "two\n" });
      yield* TestClock.adjust("1 day");
      yield* look;
      assert.lengthOf(runner.calls, 1);
      // Only a storage-limit pause resumes by itself.
      assert.strictEqual((yield* watchOf(threadId))?.status, "paused");

      next = output({ stdout: "three\n" });
      yield* orchestrator.dispatch({
        type: "thread.command-watch.resume",
        commandId: CommandId.make("resume:agent-pause"),
        threadId,
        watchId: "command-watch:a",
      });
      const resumed = yield* watchOf(threadId);
      assert.strictEqual(resumed?.status, "watching");
      assert.isUndefined(resumed?.pauseReason);
      yield* TestClock.adjust("10 minutes");
      yield* look;
      assert.lengthOf(runner.calls, 2);
      const updates = yield* updatesOf(threadId);
      assert.lengthOf(updates, 1);
      assert.include(updates[0]?.text ?? "", "+ three");

      yield* orchestrator.dispatch({
        type: "thread.command-watch.stop",
        commandId: CommandId.make("stop:agent-pause"),
        threadId,
      });
      assert.isTrue(Exit.isFailure(yield* Effect.exit(pause("ended"))));
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a detected change ends a snooze, and a quiet check does not", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:command-watch-snooze");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");
    yield* finishRuns(threadId);

    let next = output({ stdout: "one\n" });
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;

      const snoozedUntil = "2099-01-01T00:00:00.000Z";
      yield* orchestrator.dispatch({
        type: "thread.snooze",
        commandId: CommandId.make("snooze:watch"),
        threadId,
        snoozedUntil,
      });
      const snoozedUntilNow = projections
        .getThread(threadId)
        .pipe(Effect.map((thread) => thread.snoozedUntil));

      yield* TestClock.adjust("1 day");
      yield* look;
      assert.lengthOf(runner.calls, 2);
      assert.isNotNull(yield* snoozedUntilNow);

      next = output({ stdout: "two\n" });
      yield* TestClock.adjust("1 day");
      yield* look;
      assert.include((yield* updatesOf(threadId))[0]?.text ?? "", "+ two");
      assert.isNull(yield* snoozedUntilNow);
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("settling stops checks without ending the watch, and un-settling restarts them", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:command-watch-settle");
    yield* createThread(threadId);
    yield* startWatch(threadId, yield* startRun(threadId, "work"), "command-watch:a", "gh api");
    yield* finishRuns(threadId);

    let next = output({ stdout: "one\n" });
    const runner = fakeRunner(() => next);
    yield* Effect.gen(function* () {
      const reactor = yield* CommandWatchReactor.make;
      const look = reactor.sweep.pipe(Effect.andThen(reactor.awaitRuns));
      yield* look;
      assert.lengthOf(runner.calls, 1);

      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("settle:watch"),
        threadId,
      });
      const kept = yield* watchOf(threadId);
      assert.strictEqual(kept?.status, "watching");
      assert.isNotNull(kept?.authority);

      next = output({ stdout: "two\n" });
      yield* TestClock.adjust("1 day");
      yield* look;
      assert.lengthOf(runner.calls, 1);
      assert.lengthOf(yield* updatesOf(threadId), 0);

      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make("unsettle:watch"),
        threadId,
        reason: "user",
      });
      // Past the scan backstop: this reactor is not subscribed to the event that shortens it.
      yield* TestClock.adjust("10 minutes");
      yield* look;
      assert.lengthOf(runner.calls, 2);
      assert.include((yield* updatesOf(threadId))[0]?.text ?? "", "+ two");
    }).pipe(Effect.provide(runner.layer), Effect.scoped);
  }).pipe(Effect.provide(layerTest)),
);
