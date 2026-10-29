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

import { commandWatchRevisionText } from "../../../orchestration-v2/commandWatch.ts";
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
  type ReadCommandWatchChangeInput,
  type ResumeCommandWatchInput,
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
        const helpers = yield* readHelpers(cwd, input.helpers ?? []);
        const helperRefs = helpers.map(({ path: helperPath, sha256: hash }) => ({
          path: helperPath,
          sha256: hash,
        }));
        const revisionHash = yield* sha256(
          commandWatchRevisionText({ command: input.command, format, cwd, helpers: helperRefs }),
        );
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
            everyMinutes: input.everyMinutes ?? DEFAULT_EVERY_MINUTES,
            format,
            helpers: helperRefs,
            reportInitialState: input.reportInitialState ?? false,
            cwd,
            revisionHash,
            ...(input.replaces === undefined ? {} : { replacesWatchId: input.replaces }),
          })
          .pipe(Effect.catchCause(refusal));
        const watches = yield* watchesOf(caller.id);
        // An identical definition keeps the watch it already had.
        const started = watches.find(
          (watch) => watch.revisionHash === revisionHash && watch.label === input.label,
        );
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
    list_command_watches: McpToolAccess.actsAsCaller(() =>
      Effect.gen(function* () {
        yield* requireOrchestration;
        const caller = yield* requireCaller("list its command watches");
        return { watches: yield* watchesOf(caller.id) };
      }),
    ),
    resume_command_watch: McpToolAccess.actsAsCaller((input: ResumeCommandWatchInput) =>
      Effect.gen(function* () {
        yield* requireOrchestration;
        const caller = yield* requireCaller("resume its command watches");
        const commandSuffix = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
        yield* engine
          .dispatch({
            type: "thread.command-watch.resume",
            commandId: CommandId.make(`server:mcp-command-resume:${caller.id}:${commandSuffix}`),
            threadId: caller.id,
            watchId: input.watchId,
          })
          .pipe(Effect.catchCause(refusal));
        return { watches: yield* watchesOf(caller.id) };
      }),
    ),
    read_command_watch_change: McpToolAccess.actsAsCaller((input: ReadCommandWatchChangeInput) =>
      Effect.gen(function* () {
        yield* requireOrchestration;
        const caller = yield* requireCaller("read its command watch changes");
        // Only the calling thread's own watches.
        const watches = yield* watchesOf(caller.id);
        const missing = new CommandWatchFailedError({
          reason: `No stored change ${input.sequence} for watch ${input.watchId} on this thread.`,
        });
        if (!watches.some((watch) => watch.id === input.watchId)) return yield* missing;
        const change = yield* store
          .readChange(input.watchId, input.sequence)
          .pipe(Effect.mapError(() => missing));
        if (change === undefined) return yield* missing;
        return {
          sequence: change.sequence,
          summary: change.summary,
          detectedAt: change.detectedAt,
          content: change.content,
        };
      }),
    ),
  };
});

export const layer = McpToolAccess.toLayer(CommandWatchesToolkit, make);
