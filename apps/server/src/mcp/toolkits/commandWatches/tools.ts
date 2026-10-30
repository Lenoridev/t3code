import {
  OrchestratorMcpFailure,
  THREAD_COMMAND_WATCH_HELPER_LIMIT,
  ThreadCommandWatch,
  ThreadCommandWatchCommand,
  ThreadCommandWatchEveryMinutes,
  ThreadCommandWatchFormat,
  ThreadCommandWatchHelperPath,
  ThreadCommandWatchLabel,
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

export const WatchCommandInput = Schema.Struct({
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
    "Have T3 Code run a shell command for this thread on an interval and send you an update when its output changes. Use it to wait on anything that is not a pull request (a discussion or issue reply, another repository's CI, a deploy, a release) instead of polling or sleeping. Write a read-only check whose output changes only when there is news, for example the item's state plus recent comments from other accounts. Leave out your own and the user's comments in the command itself (for example `jq --arg me \"$(gh api user --jq .login)\" 'map(select(.user.login != $me))'`), so your own replies do not come back to you. The first check only records the output, so a state that already holds never wakes you, unless reportInitialState is set. Any nonzero exit is a failed check, so turn an expected empty result into successful output (`grep ... || true`). Failed checks retry with growing delays; 8 in a row pause the watch until you or the user resume it. Output over 4 KB is a failed check, so keep the output small. A check runs while you are busy; updates wait in the queue and arrive one at a time, never dropped. In Full access the watch starts at once. Otherwise the user must allow it first: the status is awaiting_approval until they do. It runs with the T3 server's environment, not yours: rely on tools already logged in on the server, and keep secrets out of the command, since the user sees it. While the watch runs, the thread stays in the user's Working list. Call unwatch_command when you no longer need it, before handing the work back. A subagent cannot watch: its parent thread does. Calling again with an identical command, format, helpers and label returns the existing watch. To change a running watch, pass replaces with its id.",
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
    "Stop a command watch on this thread, or every command watch when watchId is omitted. A stopped watch delivers nothing more: an update it queued that you have not started on is removed. Stopping a watch that already ended changes nothing.",
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
    "List this thread's command watches with their status: awaiting_approval, baselining, watching, or paused with a reason.",
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

export const CommandWatchesToolkit = Toolkit.make(
  WatchCommandTool,
  UnwatchCommandTool,
  ListCommandWatchesTool,
  ResumeCommandWatchTool,
  ReadCommandWatchChangeTool,
);
