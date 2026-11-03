import {
  CommandId,
  OrchestratorMcpFailure,
  THREAD_COMMAND_WATCH_ID_PREFIX,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import {
  commandWatchRevisionText,
  parseComparisonState,
  readComparisonResult,
} from "../../../orchestration-v2/commandWatch.ts";
import * as CommandWatchComparison from "../../../orchestration-v2/CommandWatchComparison.ts";
import { resolveCommandWatchShell } from "../../../orchestration-v2/CommandWatchReactor.ts";
import * as CommandWatchStore from "../../../orchestration-v2/CommandWatchStore.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import { threadWorkspaceRoot } from "../../../project/threadWorkspaceRoot.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { loadCaller } from "../../threadAccess.ts";
import {
  CommandWatchFailedError,
  CommandWatchesToolkit,
  type ReadCommandWatchesInput,
  type SetCommandWatchPausedInput,
  type TestCommandWatchComparisonInput,
  type UnwatchCommandInput,
  type WatchCommandInput,
} from "./tools.ts";

const DEFAULT_EVERY_MINUTES = 10;
const HELPER_LIMIT_BYTES = 64 * 1024;
const isDispatchError = Schema.is(Orchestrator.OrchestratorDispatchError);

/** What the orchestrator said when it refused, so the agent learns why. Anything else is logged. */
const refusal = <E>(cause: Cause.Cause<E>): Effect.Effect<never, CommandWatchFailedError> => {
  if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause as Cause.Cause<never>);
  const reason = Option.flatMap(Cause.findErrorOption(cause), (error) =>
    isDispatchError(error) && typeof error.cause === "string"
      ? Option.some(error.cause)
      : Option.none(),
  );
  return Option.match(reason, {
    onSome: (text) => Effect.fail(new CommandWatchFailedError({ reason: text })),
    onNone: () =>
      Effect.logWarning("command watch dispatch failed", { cause }).pipe(
        Effect.andThen(
          Effect.fail(
            new CommandWatchFailedError({ reason: "Could not change the command watch." }),
          ),
        ),
      ),
  });
};

const make = Effect.gen(function* () {
  const engine = yield* Orchestrator.OrchestratorV2;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const store = yield* CommandWatchStore.CommandWatchStore;
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const projects = yield* ProjectService.ProjectService;
  const comparison = yield* CommandWatchComparison.CommandWatchComparison;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const shell = yield* resolveCommandWatchShell;

  const sha256 = (text: string) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(text))
      .pipe(Effect.map(Hex.encode), Effect.orDie);

  const requireOrchestration = McpInvocationContext.McpInvocationContext.pipe(
    Effect.flatMap((scope) =>
      scope.capabilities.has("orchestration")
        ? Effect.void
        : Effect.fail(
            new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "This credential cannot control threads.",
            }),
          ),
    ),
  );

  const watchesOf = (threadId: ThreadId) =>
    threads.getThreadRecords(threadId, []).pipe(
      Effect.map(({ thread }) => thread.commandWatches ?? []),
      Effect.mapError(
        () => new CommandWatchFailedError({ reason: "Could not read this thread's watches." }),
      ),
    );

  const requireCaller = (action: string) =>
    loadCaller().pipe(
      Effect.flatMap(({ caller }) =>
        caller === undefined
          ? Effect.fail(
              new OrchestratorMcpFailure({
                code: "parent_not_active",
                message: `Only an agent running inside a T3 thread can ${action}.`,
              }),
            )
          : Effect.succeed(caller),
      ),
    );

  /** Copies declared helpers from inside the workspace, so the watch runs what exists now. */
  const readHelpers = (cwd: string, paths: ReadonlyArray<string>) =>
    Effect.forEach(paths, (declared) =>
      Effect.gen(function* () {
        const relative = path.normalize(declared);
        const absolute = path.resolve(cwd, relative);
        const inside = path.relative(cwd, absolute);
        if (path.isAbsolute(declared) || inside.startsWith("..") || path.isAbsolute(inside)) {
          return yield* new CommandWatchFailedError({
            reason: `Helper ${declared} must be a path inside this thread's workspace.`,
          });
        }
        const content = yield* fileSystem
          .readFileString(absolute)
          .pipe(
            Effect.mapError(
              () => new CommandWatchFailedError({ reason: `Could not read helper ${declared}.` }),
            ),
          );
        if (Buffer.byteLength(content, "utf8") > HELPER_LIMIT_BYTES) {
          return yield* new CommandWatchFailedError({
            reason: `Helper ${declared} is larger than ${HELPER_LIMIT_BYTES / 1024} KB.`,
          });
        }
        return { path: inside, content, sha256: yield* sha256(content) };
      }),
    );

  return {
    watch_command: McpToolAccess.actsAsCaller((input: WatchCommandInput) =>
      Effect.gen(function* () {
        yield* requireOrchestration;
        const caller = yield* requireCaller("watch a command");
        if (caller.activeRunId === null) {
          return yield* new OrchestratorMcpFailure({
            code: "parent_not_active",
            message: "Only an agent running inside a T3 thread can watch a command.",
          });
        }
        if (caller.lineage.relationshipToParent === "subagent") {
          return yield* new CommandWatchFailedError({
            reason:
              "This thread is a subagent, so it cannot watch commands. Finish your task and report back: the parent thread can watch.",
          });
        }
        const cwd = yield* threadWorkspaceRoot(caller.id).pipe(
          Effect.provideService(ProjectService.ProjectService, projects),
        );
        if (cwd === undefined) {
          return yield* new CommandWatchFailedError({
            reason: "This thread has no workspace directory to run the command in.",
          });
        }
        const format = input.format ?? "text";
        if (input.completionCondition !== undefined && input.comparisonSource === undefined) {
          return yield* new CommandWatchFailedError({
            reason:
              "Only a comparison can end a watch, so completionCondition needs comparisonSource.",
          });
        }
        const existing = (yield* watchesOf(caller.id)).find(
          (watch) => watch.clientRequestId === input.requestId,
        );
        const source = input.comparisonSource;
        if (source !== undefined) {
          // No fallback: without the restricted runtime, custom comparison is unavailable.
          yield* comparison.compile(source).pipe(
            Effect.mapError(
              (error) =>
                new CommandWatchFailedError({
                  reason: error.unavailable
                    ? `Custom comparison is unavailable on this server: ${error.reason}. Omit comparisonSource to compare output exactly.`
                    : `The comparison source does not compile: ${error.reason}`,
                }),
            ),
          );
        }
        const helpers = yield* readHelpers(cwd, input.helpers ?? []);
        const helperRefs = helpers.map(({ path: helperPath, sha256: hash }) => ({
          path: helperPath,
          sha256: hash,
        }));
        const everyMinutes = input.everyMinutes ?? DEFAULT_EVERY_MINUTES;
        const reportInitialState = input.reportInitialState ?? false;
        const completion =
          input.completionCondition === undefined
            ? ({ kind: "ongoing" } as const)
            : ({ kind: "condition", description: input.completionCondition } as const);
        const sourceHash = source === undefined ? null : yield* sha256(source);
        const revisionHash = yield* sha256(
          commandWatchRevisionText({
            environmentId: yield* environment.getEnvironmentId,
            threadId: caller.id,
            command: input.command,
            format,
            cwd,
            shell: shell.program,
            comparisonSourceSha256: sourceHash,
            completion,
            everyMinutes,
            reportInitialState,
            helpers: helperRefs,
          }),
        );
        if (existing !== undefined) {
          if (existing.revisionHash === revisionHash && existing.label === input.label) {
            return { watchId: existing.id, watches: yield* watchesOf(caller.id) };
          }
          return yield* new CommandWatchFailedError({
            reason: `The request id ${input.requestId} already created a different watch. Use a new id for a new watch.`,
          });
        }
        const watchId = `${THREAD_COMMAND_WATCH_ID_PREFIX}${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`;
        // Stored before the watch exists, so its first check finds the helper copies.
        yield* store
          .create({
            watchId,
            threadId: caller.id,
            helpers: helpers.map(({ path: helperPath, content }) => ({
              path: helperPath,
              content,
            })),
            comparisonSource: source,
            now: DateTime.formatIso(yield* DateTime.now),
          })
          .pipe(
            Effect.mapError(
              () => new CommandWatchFailedError({ reason: "Could not store the watch." }),
            ),
          );
        yield* engine
          .dispatch({
            type: "thread.command-watch.start",
            commandId: CommandId.make(`server:mcp-command-watch:${caller.id}:${watchId}`),
            threadId: caller.id,
            callerRunId: caller.activeRunId,
            watchId,
            command: input.command,
            label: input.label,
            everyMinutes,
            format,
            helpers: helperRefs,
            reportInitialState,
            cwd,
            shell: shell.program,
            comparison:
              sourceHash !== null
                ? { kind: "custom-js-v1", sha256: sourceHash }
                : { kind: format === "json" ? "json-v1" : "text-v1" },
            completion,
            clientRequestId: input.requestId,
            revisionHash,
            ...(input.replaces === undefined ? {} : { replacesWatchId: input.replaces }),
          })
          .pipe(Effect.catchCause(refusal));
        const watches = yield* watchesOf(caller.id);
        // A retry that lost a race keeps the watch the first call created.
        const started = watches.find((watch) => watch.clientRequestId === input.requestId);
        return {
          ...(started === undefined ? {} : { watchId: started.id }),
          watches,
        };
      }),
    ),
    unwatch_command: McpToolAccess.actsAsCaller((input: UnwatchCommandInput) =>
      Effect.gen(function* () {
        yield* requireOrchestration;
        const caller = yield* requireCaller("stop its command watches");
        const commandSuffix = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
        yield* engine
          .dispatch({
            type: "thread.command-watch.stop",
            commandId: CommandId.make(`server:mcp-command-unwatch:${caller.id}:${commandSuffix}`),
            threadId: caller.id,
            ...(input.watchId === undefined ? {} : { watchId: input.watchId }),
          })
          .pipe(Effect.catchCause(refusal));
        return { watches: yield* watchesOf(caller.id) };
      }),
    ),
    read_command_watches: McpToolAccess.actsAsCaller((input: ReadCommandWatchesInput) =>
      Effect.gen(function* () {
        yield* requireOrchestration;
        const caller = yield* requireCaller("read its command watches");
        const watches = yield* watchesOf(caller.id);
        if (input.watchId === undefined) {
          if (input.sequence !== undefined) {
            return yield* new CommandWatchFailedError({ reason: "sequence needs watchId." });
          }
          return { watches };
        }
        // Only the calling thread's own watches.
        const watch = watches.find(({ id }) => id === input.watchId);
        const missing = new CommandWatchFailedError({
          reason: `No watch ${input.watchId} on this thread.`,
        });
        if (watch === undefined) return yield* missing;
        if (input.sequence !== undefined) {
          const absent = new CommandWatchFailedError({
            reason: `No stored change ${input.sequence} for watch ${watch.id}.`,
          });
          const change = yield* store
            .readChange(watch.id, input.sequence)
            .pipe(Effect.mapError(() => absent));
          if (change === undefined) return yield* absent;
          const { sequence, summary, detectedAt, content } = change;
          return { watches: [watch], change: { sequence, summary, detectedAt, content } };
        }
        const storage = yield* store.get(watch.id).pipe(Effect.mapError(() => missing));
        return {
          watches: [watch],
          details: {
            comparisonSource: storage?.comparisonSource ?? null,
            helpers: storage?.helpers ?? [],
            acceptedOutput: storage?.observation ?? null,
            comparisonState: storage?.comparisonState ?? null,
          },
        };
      }),
    ),
    set_command_watch_paused: McpToolAccess.actsAsCaller((input: SetCommandWatchPausedInput) =>
      Effect.gen(function* () {
        yield* requireOrchestration;
        const caller = yield* requireCaller("pause or resume its command watches");
        if (!input.paused) {
          const watch = (yield* watchesOf(caller.id)).find(({ id }) => id === input.watchId);
          if (watch?.pauseReason === "user") {
            return yield* new CommandWatchFailedError({
              reason: `The user paused watch ${input.watchId}. Only they can resume it.`,
            });
          }
        }
        const suffix = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
        const commandId = CommandId.make(`server:mcp-command-pause:${caller.id}:${suffix}`);
        const target = { commandId, threadId: caller.id, watchId: input.watchId };
        yield* engine
          .dispatch(
            input.paused
              ? { type: "thread.command-watch.pause", ...target, pausedBy: "agent" }
              : { type: "thread.command-watch.resume", ...target },
          )
          .pipe(Effect.catchCause(refusal));
        return { watches: yield* watchesOf(caller.id) };
      }),
    ),
    test_command_watch_comparison: McpToolAccess.actsAsCaller(
      (input: TestCommandWatchComparisonInput) =>
        Effect.gen(function* () {
          yield* requireOrchestration;
          const phase = input.phase ?? (input.previous === undefined ? "baseline" : "update");
          return yield* comparison
            .compare(input.source, {
              phase,
              previous: input.previous ?? null,
              current: input.current,
              state: input.state ?? null,
            })
            .pipe(
              Effect.map((resultJson) => {
                const outcome = readComparisonResult({
                  resultJson,
                  observation: "",
                  phase,
                  // A test shows what the comparison reported, even on a baseline.
                  reportInitialState: true,
                  completion:
                    input.completionCondition === true
                      ? { kind: "condition", description: "test" }
                      : { kind: "ongoing" },
                });
                if (outcome.kind === "failed") return { ok: false, error: outcome.reason };
                return {
                  ok: true,
                  state: parseComparisonState(outcome.state ?? null),
                  changes: outcome.changes
                    .filter((change) => change.kind !== "completed")
                    .map(({ kind, summary, content }) => ({ kind, summary, details: content })),
                  ...(outcome.completionReason === undefined
                    ? {}
                    : { completionReason: outcome.completionReason }),
                };
              }),
              Effect.catch((error) =>
                Effect.succeed({ ok: false, error: `comparison ${error.reason}` }),
              ),
            );
        }),
    ),
  };
});

export const layer = McpToolAccess.toLayer(CommandWatchesToolkit, make);
