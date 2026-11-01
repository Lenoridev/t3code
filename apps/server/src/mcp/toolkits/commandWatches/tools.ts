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
  "Define one synchronous `function compare({ phase, previous, current, state })` in plain JavaScript. phase is baseline on the first check, when previous and state are null, then update. previous and current are the output: a string with format text, a value with format json. Return { state, changes, complete? }: state is JSON you keep for the next run (null for none, at most 8 KB); changes is a list of { key, kind, summary, details }, where key is unique in this result, kind names the category such as comment.added, summary is plain text of at most 120 characters, and details is JSON evidence; an empty list wakes nobody. It runs in a restricted interpreter with no imports, require, filesystem, network, timers, async, current time, or randomness, within 250 ms and 32 MB. Date accepts explicit values only, in UTC. Throwing fails the check without replacing the last accepted output.";

export const WatchCommandInput = Schema.Struct({
  requestId: ThreadCommandWatchRequestId.annotate({
    description:
      "An id you make up for this request. Retrying with the same id and definition returns the same watch, and the same id with a different definition is refused, so use a new id for each new watch.",
  }),
  command: ThreadCommandWatchCommand.annotate({
    description:
      "Shell command to run, for example a gh or curl query filtered with jq. It runs in this thread's workspace with the T3 server's environment. Refer to a declared helper script as \"$T3_WATCH_HELPERS/<its path>\".",
  }),
  label: ThreadCommandWatchLabel.annotate({
    description: "Short name the user sees for this watch, for example 'Maintainer reply on #123'.",
  }),
  everyMinutes: Schema.optional(
    ThreadCommandWatchEveryMinutes.annotate({
      description:
        "Minutes from the end of one check to the start of the next, 1 to 1440. Defaults to 10.",
    }),
  ),
  format: Schema.optional(
    ThreadCommandWatchFormat.annotate({
      description:
        "How output is compared. text (default) compares it exactly, whitespace included. json parses one JSON value and ignores formatting and object key order, but not array order.",
    }),
  ),
  helpers: Schema.optional(
    Schema.Array(ThreadCommandWatchHelperPath)
      .check(Schema.isMaxLength(THREAD_COMMAND_WATCH_HELPER_LIMIT))
      .annotate({
        description: `Workspace-relative paths of scripts the command runs, at most ${THREAD_COMMAND_WATCH_HELPER_LIMIT}. T3 Code stores a copy now and runs that copy, so later edits to the files do not change the watch. Files the command only reads as data are not helpers.`,
      }),
  ),
  reportInitialState: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "Report what the first check prints instead of only recording it. Defaults to false.",
    }),
  ),
  comparisonSource: Schema.optional(
    ComparisonSource.annotate({
      description: `JavaScript that decides which changes matter, for when any output change is not news, such as ignoring your own comments or naming what happened. Test it first with test_command_watch_comparison, whose description gives the contract. Omit it to compare output exactly.`,
    }),
  ),
  completionCondition: Schema.optional(
    ThreadCommandWatchCondition.annotate({
      description:
        "When this watch should end on its own, for example 'CI on main finished'. Only with comparisonSource: it ends the watch by returning complete: { reason }. Omit it for ongoing monitoring, which runs until stopped.",
    }),
  ),
  replaces: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "A watch on this thread this one replaces. The new watch starts from a fresh first check; outside Full access the old one keeps running until the user allows the new one. Changes the old one has not delivered move to the new one.",
    }),
  ),
});
export type WatchCommandInput = typeof WatchCommandInput.Type;

export const UnwatchCommandInput = Schema.Struct({
  watchId: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "Watch to stop. Omit to stop every command watch on this thread.",
    }),
  ),
});
export type UnwatchCommandInput = typeof UnwatchCommandInput.Type;

export const ResumeCommandWatchInput = Schema.Struct({
  watchId: TrimmedNonEmptyString.annotate({ description: "The paused watch to resume." }),
});
export type ResumeCommandWatchInput = typeof ResumeCommandWatchInput.Type;

export const ReadCommandWatchChangeInput = Schema.Struct({
  watchId: TrimmedNonEmptyString,
  sequence: Schema.Int.annotate({ description: "The change number an update named." }),
});
export type ReadCommandWatchChangeInput = typeof ReadCommandWatchChangeInput.Type;

export const ReadCommandWatchInput = Schema.Struct({
  watchId: TrimmedNonEmptyString,
});
export type ReadCommandWatchInput = typeof ReadCommandWatchInput.Type;

export const CommandWatchDetailsResult = Schema.Struct({
  watch: ThreadCommandWatch,
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
  previous: Schema.optional(
    Schema.Unknown.annotate({ description: "Sample previous output: a string, or a JSON value." }),
  ),
  current: Schema.Unknown.annotate({ description: "Sample current output." }),
  state: Schema.optional(Schema.Unknown.annotate({ description: "Sample saved state." })),
  completionCondition: Schema.optional(
    Schema.Boolean.annotate({
      description: "Whether the watch would declare a completion condition. Defaults to false.",
    }),
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

const WatchCommandTool = Tool.make("watch_command", {
  description:
    "Have T3 Code run a shell command for this thread on an interval and send you an update when its output changes. Use it to wait on anything that is not a pull request (a discussion or issue reply, another repository's CI, a deploy, a release) instead of polling or sleeping. Write a read-only check whose output changes only when there is news, for example the item's state plus recent comments from other accounts. Leave out your own and the user's comments in the command itself (for example `jq --arg me \"$(gh api user --jq .login)\" 'map(select(.user.login != $me))'`), so your own replies do not come back to you. The first check only records the output, so a state that already holds never wakes you, unless reportInitialState is set. Any nonzero exit is a failed check, so turn an expected empty result into successful output (`grep ... || true`). Failed checks retry with growing delays; 8 in a row pause the watch until you or the user resume it. Output over 4 KB is a failed check, so keep the output small. A check runs while you are busy; updates wait in the queue and arrive one at a time, never dropped. In Full access the watch starts at once. Otherwise the user must allow it first: the status is awaiting_approval until they do. It runs with the T3 server's environment, not yours: rely on tools already logged in on the server, and keep secrets out of the command, since the user sees it. While the watch runs, the thread stays in the user's Working list. If the user settles the thread, checks wait until it is un-settled; archiving ends the watch. Call unwatch_command when you no longer need it, before handing the work back. A subagent cannot watch: its parent thread does. To change a running watch, pass replaces with its id. A check that fails to compare keeps the last accepted output. Pass comparisonSource when only some output changes are news.",
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
    "Stop a command watch on this thread, or every command watch when watchId is omitted. A stopped watch delivers nothing more: an update it queued that you have not started on is removed, and changes still waiting are discarded. Stopping a watch that already ended changes nothing.",
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

const ListCommandWatchesTool = Tool.make("list_command_watches", {
  description:
    "List this thread's command watches with their status: awaiting_approval, baselining, watching, paused with a reason, or ended with a reason. Recently ended watches stay listed.",
  parameters: Tool.EmptyParams,
  success: CommandWatchesResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "List command watches")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const ResumeCommandWatchTool = Tool.make("resume_command_watch", {
  description:
    "Resume a paused command watch on this thread. It continues from the last accepted output, so it reports what changed while it was paused as one change.",
  parameters: ResumeCommandWatchInput,
  success: CommandWatchesResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "Resume command watch")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const ReadCommandWatchChangeTool = Tool.make("read_command_watch_change", {
  description:
    "Read the full text of a change a command watch update shortened. The update names the watch and change number.",
  parameters: ReadCommandWatchChangeInput,
  success: CommandWatchChangeResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "Read command watch change")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const ReadCommandWatchTool = Tool.make("read_command_watch", {
  description:
    "Read one of this thread's command watches in full: its definition, stored helper scripts, comparison source and saved state, the last accepted output, and its failures.",
  parameters: ReadCommandWatchInput,
  success: CommandWatchDetailsResult,
  failure: CommandWatchToolError,
  dependencies,
})
  .annotate(Tool.Title, "Read command watch")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const TestCommandWatchComparisonTool = Tool.make("test_command_watch_comparison", {
  description: `Try comparison source on sample output before using it in watch_command. It runs the same restricted interpreter and result checks as a live check, runs no command, and sends nothing. ${COMPARISON_CONTRACT}`,
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
  ListCommandWatchesTool,
  ResumeCommandWatchTool,
  ReadCommandWatchChangeTool,
  ReadCommandWatchTool,
  TestCommandWatchComparisonTool,
);
