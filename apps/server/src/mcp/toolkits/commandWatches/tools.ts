import {
  OrchestratorMcpFailure,
  THREAD_COMMAND_WATCH_COMPARISON_SOURCE_LIMIT_BYTES,
  THREAD_COMMAND_WATCH_HELPER_LIMIT,
  ThreadCommandWatch,
  ThreadCommandWatchCommand,
  ThreadCommandWatchCondition,
  ThreadCommandWatchDetails,
  ThreadCommandWatchEveryMinutes,
  ThreadCommandWatchFormat,
  ThreadCommandWatchHelperPath,
  ThreadCommandWatchLabel,
  ThreadCommandWatchRequestId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  Orchestrator.OrchestratorV2,
];

export class CommandWatchFailedError extends Schema.TaggedError<CommandWatchFailedError>()(
  "CommandWatchFailedError",
  { reason: Schema.String },
) {
  override get message(): string {
    return this.reason;
  }
}

export const CommandWatchToolError = Schema.Union([
  OrchestratorMcpFailure,
  CommandWatchFailedError,
]);
export type CommandWatchToolError = typeof CommandWatchToolError.Type;

const ComparisonSource = Schema.String.check(
  Schema.isMaxLength(THREAD_COMMAND_WATCH_COMPARISON_SOURCE_LIMIT_BYTES),
);

const COMPARISON_CONTRACT =
  "Define one synchronous `function compare({ phase, previous, current, state })` in plain JavaScript. phase is baseline on the first check (previous and state are null), then update. previous and current are the output: a string with format text, a value with format json. Return { state, changes, complete? }: state is JSON kept for the next run (null for none, at most 8 KB); changes is a list of { key, kind, summary, details }, with key unique in this result, kind a category such as comment.added, summary plain text of at most 120 characters, and details JSON evidence; an empty list wakes nobody. complete: { reason } ends a watch that has a completionCondition. No imports, filesystem, network, timers, async, current time, or randomness; 250 ms and 32 MB. Date takes explicit values only, in UTC. Throwing fails the check and keeps the last accepted output.";

export const WatchCommandInput = Schema.Struct({
  requestId: ThreadCommandWatchRequestId,
  command: ThreadCommandWatchCommand,
  label: ThreadCommandWatchLabel,
  everyMinutes: Schema.optional(
    ThreadCommandWatchEveryMinutes.annotate({
      description: "Minutes between checks, 1 to 1440. Defaults to 10.",
    }),
  ),
  format: Schema.optional(
    ThreadCommandWatchFormat.annotate({
      description:
        "text (default) compares output exactly. json parses one JSON value and ignores formatting and key order, but not array order.",
    }),
  ),
  helpers: Schema.optional(
    Schema.Array(ThreadCommandWatchHelperPath)
      .check(Schema.isMaxLength(THREAD_COMMAND_WATCH_HELPER_LIMIT))
      .annotate({
        description: `Workspace-relative paths of scripts the command runs, at most ${THREAD_COMMAND_WATCH_HELPER_LIMIT}. T3 Code runs a copy stored now, so later edits do not change the watch. Refer to one as "$T3_WATCH_HELPERS/<its path>".`,
      }),
  ),
  reportInitialState: Schema.optional(
    Schema.Boolean.annotate({
      description: "Report what the first check prints instead of only recording it.",
    }),
  ),
  comparisonSource: Schema.optional(
    ComparisonSource.annotate({
      description: `JavaScript that decides which changes are news. Test it first with test_command_watch_comparison, whose description gives the contract.`,
    }),
  ),
  completionCondition: Schema.optional(ThreadCommandWatchCondition),
  replaces: Schema.optional(TrimmedNonEmptyString),
});
export type WatchCommandInput = typeof WatchCommandInput.Type;

export const UnwatchCommandInput = Schema.Struct({
  watchId: Schema.optional(TrimmedNonEmptyString),
});
export type UnwatchCommandInput = typeof UnwatchCommandInput.Type;

export const SetCommandWatchPausedInput = Schema.Struct({
  watchId: TrimmedNonEmptyString,
  paused: Schema.Boolean,
});
export type SetCommandWatchPausedInput = typeof SetCommandWatchPausedInput.Type;

export const ReadCommandWatchesInput = Schema.Struct({
  watchId: Schema.optional(TrimmedNonEmptyString),
  sequence: Schema.optional(
    Schema.Int.annotate({ description: "A change number an update named. Needs watchId." }),
  ),
});
export type ReadCommandWatchesInput = typeof ReadCommandWatchesInput.Type;

export const CommandWatchDetailsResult = Schema.Struct({
  ...ThreadCommandWatchDetails.fields,
  /** The last accepted output, null until a check succeeds. */
  acceptedOutput: Schema.NullOr(Schema.String),
  /** JSON the comparison kept, null when it keeps none. */
  comparisonState: Schema.NullOr(Schema.String),
});

export const TestCommandWatchComparisonInput = Schema.Struct({
  source: ComparisonSource.annotate({ description: "The comparison to test." }),
  phase: Schema.optional(
    Schema.Literals(["baseline", "update"]).annotate({
      description: "Defaults to update, or baseline when previous is omitted.",
    }),
  ),
  previous: Schema.optional(Schema.Unknown),
  current: Schema.Unknown,
  state: Schema.optional(Schema.Unknown),
  completionCondition: Schema.optional(
    Schema.Boolean.annotate({ description: "Whether the watch would declare one." }),
  ),
});
export type TestCommandWatchComparisonInput = typeof TestCommandWatchComparisonInput.Type;

export const TestCommandWatchComparisonResult = Schema.Struct({
  /** False when a live check would have failed; `error` says why. */
  ok: Schema.Boolean,
  error: Schema.optional(Schema.String),
  state: Schema.optional(Schema.Unknown),
  changes: Schema.optional(
    Schema.Array(
      Schema.Struct({ kind: Schema.String, summary: Schema.String, details: Schema.String }),
    ),
  ),
  completionReason: Schema.optional(Schema.String),
});

export const CommandWatchesResult = Schema.Struct({
  /** The watch this call started or found, for `watch_command`. */
  watchId: Schema.optional(Schema.String),
  watches: Schema.Array(ThreadCommandWatch),
});
export type CommandWatchesResult = typeof CommandWatchesResult.Type;

export const CommandWatchChangeResult = Schema.Struct({
  sequence: Schema.Int,
  summary: Schema.String,
  detectedAt: Schema.String,
  content: Schema.String,
});

export const ReadCommandWatchesResult = Schema.Struct({
  watches: Schema.Array(ThreadCommandWatch),
  /** With watchId: its stored definition and last accepted output. */
  details: Schema.optional(CommandWatchDetailsResult),
  /** With watchId and sequence: that change in full. */
  change: Schema.optional(CommandWatchChangeResult),
});

const WatchCommandTool = Tool.make("watch_command", {
  description:
    "Have T3 Code run a shell command in this thread's workspace on an interval and send you an update when its output changes. Use it to wait on anything that is not a pull request (a discussion or issue reply, another repository's CI, a deploy, a release) instead of polling or sleeping. Write a read-only check whose output changes only when there is news, and leave out your own and the user's comments, for example `jq --arg me \"$(gh api user --jq .login)\" 'map(select(.user.login != $me))'`. Use a new requestId for each new watch; retrying with the same one returns the same watch. The first check only records the output. A nonzero exit is a failed check, so turn an expected empty result into success (`grep ... || true`). Output over 4 KB, or 64 KB with comparisonSource, also fails. Failures retry with growing delays; you hear after 3 in a row, and 8 pause the watch. Updates queue while you are busy and are never dropped. In Full access the watch starts at once; otherwise its status is awaiting_approval until the user allows it. It runs with the T3 server's environment: rely on tools logged in there, and keep secrets out of the command. Checks wait while the thread is settled, and archiving ends the watch. Call unwatch_command before handing the work back. A subagent cannot watch. To change a watch, pass replaces with its id: the new one starts from a fresh first check and takes over undelivered changes, and outside Full access the old one runs until the user allows the new one. completionCondition (needs comparisonSource) says when the watch ends on its own; without it, the watch runs until stopped.",
  parameters: WatchCommandInput,
  success: CommandWatchesResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "Watch command")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

const UnwatchCommandTool = Tool.make("unwatch_command", {
  description:
    "Stop a command watch on this thread, or all of them when watchId is omitted. It delivers nothing more: a queued update and waiting changes are discarded.",
  parameters: UnwatchCommandInput,
  success: CommandWatchesResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "Stop watching command")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const ReadCommandWatchesTool = Tool.make("read_command_watches", {
  description:
    "List this thread's command watches with their status: awaiting_approval, baselining, watching, paused, or ended, with a reason. With watchId, also its stored helpers, comparison source and state, and last accepted output. With watchId and sequence, a change in full, for an update that shortened it.",
  parameters: ReadCommandWatchesInput,
  success: ReadCommandWatchesResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "Read command watches")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const SetCommandWatchPausedTool = Tool.make("set_command_watch_paused", {
  description:
    "Pause or resume a command watch. A pause stops its checks until you resume it: nothing resumes it for you, the user sees it paused, and changes already detected still arrive. Use it around noise you will cause yourself. A watch the user paused is theirs to resume. Resuming reports what changed meanwhile as one change.",
  parameters: SetCommandWatchPausedInput,
  success: CommandWatchesResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "Pause or resume command watch")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const TestCommandWatchComparisonTool = Tool.make("test_command_watch_comparison", {
  description: `Try comparison source on sample output, with the same checks as a live run. ${COMPARISON_CONTRACT}`,
  parameters: TestCommandWatchComparisonInput,
  success: TestCommandWatchComparisonResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "Test command watch comparison")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const CommandWatchesToolkit = Toolkit.make(
  WatchCommandTool,
  UnwatchCommandTool,
  ReadCommandWatchesTool,
  SetCommandWatchPausedTool,
  TestCommandWatchComparisonTool,
);
