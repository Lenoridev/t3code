// @effect-diagnostics nodeBuiltinImport:off
import {
  EnvironmentId,
  type OrchestrationV2ServerCommand,
  ProviderInstanceId,
  RunId,
  type RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import type { Tool } from "effect/ai";

import type * as CommandWatchStore from "../../../orchestration-v2/CommandWatchStore.ts";
import { CommandWatchStore as CommandWatchStoreService } from "../../../orchestration-v2/CommandWatchStore.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as CommandWatchComparison from "../../../orchestration-v2/CommandWatchComparison.ts";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as McpToolAccessTestkit from "../../McpToolAccess.testkit.ts";
import * as CommandWatchesHandlers from "./handlers.ts";
import { CommandWatchesToolkit } from "./tools.ts";

const THREAD_ID = ThreadId.make("thread:command-watch-mcp");
const WORKSPACE = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-watch-mcp-"));
NodeFS.writeFileSync(NodePath.join(WORKSPACE, "check.sh"), "echo checked\n");
const OUTSIDE = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-watch-mcp-outside-"));
NodeFS.writeFileSync(NodePath.join(OUTSIDE, "secret.txt"), "not the agent's\n");
NodeFS.symlinkSync(NodePath.join(OUTSIDE, "secret.txt"), NodePath.join(WORKSPACE, "linked.sh"));

const testCrypto = Crypto.make({
  randomBytes: (size) => new Uint8Array(size).fill(7),
  digest: (_algorithm, data) => Effect.succeed(data),
});

const threadCaller: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment-1"),
  requestNamespace: "provider-session-1",
  thread: {
    threadId: THREAD_ID,
    providerSessionId: "provider-session-1",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

const makeHarness = Effect.fn("makeCommandWatchesHarness")(function* (
  options: {
    readonly runtimeMode?: RuntimeMode;
    readonly subagent?: boolean;
    readonly reject?: string;
    readonly watches?: ReadonlyArray<{ readonly id: string; readonly pauseReason: "user" }>;
  } = {},
) {
  const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
  const stored = yield* Ref.make<
    ReadonlyArray<ReadonlyArray<CommandWatchStore.CommandWatchHelperFile>>
  >([]);
  const shell = {
    ...McpToolAccessTestkit.liveThreadShell(
      THREAD_ID,
      options.runtimeMode === undefined ? {} : { runtimeMode: options.runtimeMode },
    ),
    worktreePath: WORKSPACE,
    ...(options.subagent === true
      ? {
          lineage: {
            rootThreadId: ThreadId.make("thread:parent"),
            parentThreadId: ThreadId.make("thread:parent"),
            relationshipToParent: "subagent" as const,
          },
        }
      : {}),
  };
  const layerDependencies = Layer.mergeAll(
    Layer.mock(ThreadManagement.ThreadManagementService)({
      getThreadShell: () => Effect.succeed(shell),
      getThreadRecords: () =>
        Effect.succeed({
          thread: {
            ...McpToolAccessTestkit.idleThreadProjection(shell).thread,
            commandWatches: options.watches ?? [],
          },
        }) as never,
    }),
    Layer.mock(Orchestrator.OrchestratorV2)({
      dispatch: (command) =>
        options.reject === undefined
          ? Ref.update(commands, (recorded) => [...recorded, command]).pipe(
              Effect.as({ sequence: 1, storedEvents: [] }),
            )
          : Effect.fail(
              new Orchestrator.OrchestratorDispatchError({
                commandId: command.commandId,
                commandType: command.type,
                cause: options.reject,
              }),
            ),
    }),
    Layer.succeed(Crypto.Crypto, testCrypto),
    Layer.mock(CommandWatchStoreService)({
      create: ({ helpers }) => Ref.update(stored, (recorded) => [...recorded, helpers]),
    }),
    Layer.mock(ProjectService.ProjectService)({}),
    Layer.mock(CommandWatchComparison.CommandWatchComparison)({
      compile: (source) =>
        source.includes("function compare")
          ? Effect.void
          : Effect.fail(
              new CommandWatchComparison.CommandWatchComparisonError({
                reason: "The source must define a function named compare.",
                unavailable: false,
              }),
            ),
      compare: () => Effect.succeed('{"state":null,"changes":[]}'),
    }),
    Layer.mock(ServerEnvironment.ServerEnvironment)({
      getEnvironmentId: Effect.succeed(EnvironmentId.make("environment:test")),
    }),
    NodeServices.layer,
  );
  const toolkit = yield* CommandWatchesToolkit.pipe(
    Effect.provide(
      McpToolAccess.HandlersLayer.layer(CommandWatchesHandlers.layer).pipe(
        Layer.provide(layerDependencies),
      ),
    ),
  );
  const call = <Name extends keyof typeof CommandWatchesToolkit.tools>(
    name: Name,
    params: Parameters<typeof toolkit.handle<Name>>[1],
    scope: McpInvocationContext.McpInvocationScope = threadCaller,
  ) =>
    toolkit.handle(name, params).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map(
        (chunk) => chunk.at(-1)!.result as Tool.Success<(typeof CommandWatchesToolkit.tools)[Name]>,
      ),
      Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
      Effect.provide(layerDependencies),
    );
  return { commands, stored, call };
});

describe("command watch toolkit handlers", () => {
  it.effect("starts a watch in the thread's workspace and stores its helpers first", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* harness.call("watch_command", {
        requestId: "r1",
        command: 'sh "$T3_WATCH_HELPERS/check.sh"',
        label: "Reply",
        helpers: ["check.sh"],
      });
      expect(yield* Ref.get(harness.stored)).toEqual([
        [{ path: "check.sh", content: "echo checked\n" }],
      ]);
      expect(yield* Ref.get(harness.commands)).toMatchObject([
        {
          type: "thread.command-watch.start",
          threadId: THREAD_ID,
          callerRunId: RunId.make("run:mcp-test"),
          label: "Reply",
          everyMinutes: 10,
          format: "text",
          cwd: WORKSPACE,
          helpers: [{ path: "check.sh" }],
          reportInitialState: false,
        },
      ]);
    }),
  );

  it.effect("stores comparison source under a hash and rejects source that does not compile", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* harness.call("watch_command", {
        requestId: "r1",
        command: "gh api",
        label: "Reply",
        format: "json",
        comparisonSource: "function compare() { return { state: null, changes: [] }; }",
        completionCondition: "the issue closes",
      });
      expect(yield* Ref.get(harness.commands)).toMatchObject([
        {
          comparison: { kind: "custom-js-v1" },
          completion: { kind: "condition", description: "the issue closes" },
          clientRequestId: "r1",
        },
      ]);

      const broken = yield* makeHarness();
      const error = yield* broken
        .call("watch_command", {
          requestId: "r2",
          command: "gh api",
          label: "Reply",
          comparisonSource: "not javascript",
        })
        .pipe(Effect.flip);
      expect(error).toMatchObject({ _tag: "CommandWatchFailedError" });
      expect(yield* Ref.get(broken.commands)).toEqual([]);

      const conditionOnly = yield* makeHarness();
      expect(
        yield* conditionOnly
          .call("watch_command", {
            requestId: "r3",
            command: "gh api",
            label: "Reply",
            completionCondition: "done",
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "CommandWatchFailedError" });
    }),
  );

  it.effect("leaves approval to the server outside Full access", () =>
    Effect.gen(function* () {
      const supervised = yield* makeHarness({ runtimeMode: "approval-required" });
      yield* supervised.call("watch_command", {
        requestId: "r1",
        command: "gh api",
        label: "Reply",
      });
      expect(yield* Ref.get(supervised.commands)).toMatchObject([
        { type: "thread.command-watch.start" },
      ]);
    }),
  );

  it.effect("refuses a helper outside the workspace, a subagent, and an outside client", () =>
    Effect.gen(function* () {
      const escaping = yield* makeHarness();
      expect(
        yield* escaping
          .call("watch_command", {
            requestId: "r1",
            command: "gh api",
            label: "Reply",
            helpers: ["../x.sh"],
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "CommandWatchFailedError" });
      expect(yield* Ref.get(escaping.commands)).toEqual([]);
      const linked = yield* makeHarness({ runtimeMode: "approval-required" });
      expect(
        yield* linked
          .call("watch_command", {
            requestId: "r1",
            command: "gh api",
            label: "Reply",
            helpers: ["linked.sh"],
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "CommandWatchFailedError" });
      expect(yield* Ref.get(linked.stored)).toEqual([]);

      const subagent = yield* makeHarness({ subagent: true });
      expect(
        yield* subagent
          .call("watch_command", { requestId: "r1", command: "gh api", label: "Reply" })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "CommandWatchFailedError" });

      const outside = yield* makeHarness();
      const error = yield* outside
        .call(
          "watch_command",
          { requestId: "r1", command: "gh api", label: "Reply" },
          {
            ...threadCaller,
            thread: undefined,
            client: { sessionId: "client-1", label: "Client", access: "full-access" },
          },
        )
        .pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "OrchestratorMcpFailure",
        code: "thread_credential_required",
      });
      expect(yield* Ref.get(outside.commands)).toEqual([]);
    }),
  );

  it.effect("tells the agent why the server refused", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ reject: "Thread already watches 5 commands." });
      expect(
        yield* harness
          .call("watch_command", { requestId: "r1", command: "gh api", label: "Reply" })
          .pipe(Effect.flip),
      ).toMatchObject({
        _tag: "CommandWatchFailedError",
        reason: "Thread already watches 5 commands.",
      });
    }),
  );

  it.effect("stops every watch when no id is given", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* harness.call("unwatch_command", {});
      const [stop] = yield* Ref.get(harness.commands);
      expect(stop).toMatchObject({ type: "thread.command-watch.stop", threadId: THREAD_ID });
      expect(stop).not.toHaveProperty("watchId");
    }),
  );

  it.effect("pauses the named watch on the calling thread", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* harness.call("set_command_watch_paused", {
        watchId: "command-watch:a",
        paused: true,
      });
      const [pause] = yield* Ref.get(harness.commands);
      expect(pause).toMatchObject({
        type: "thread.command-watch.pause",
        threadId: THREAD_ID,
        watchId: "command-watch:a",
        pausedBy: "agent",
      });
    }),
  );

  it.effect("refuses to resume a watch the user paused", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        watches: [{ id: "command-watch:a", pauseReason: "user" }],
      });
      expect(
        yield* harness
          .call("set_command_watch_paused", { watchId: "command-watch:a", paused: false })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: expect.stringContaining("Only they can resume it") });
      expect(yield* Ref.get(harness.commands)).toEqual([]);
    }),
  );
});
