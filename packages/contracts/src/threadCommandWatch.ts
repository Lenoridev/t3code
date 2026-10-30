import * as Schema from "effect/Schema";

import { IsoDateTime, MessageId, NonNegativeInt, TrimmedNonEmptyString } from "./baseSchemas.ts";

const THREAD_COMMAND_WATCH_MAX_COMMAND_LENGTH = 4_000;
const THREAD_COMMAND_WATCH_MAX_LABEL_LENGTH = 120;
const THREAD_COMMAND_WATCH_MAX_EVERY_MINUTES = 24 * 60;
const THREAD_COMMAND_WATCH_MAX_HELPER_PATH_LENGTH = 1_000;
/** Every watch id starts with this, so a watch id never collides with a provider task id. */
export const THREAD_COMMAND_WATCH_ID_PREFIX = "command-watch:";
/** Active watches one thread may hold at once. */
export const THREAD_COMMAND_WATCH_LIMIT = 5;
/** Helper scripts one watch may declare. */
export const THREAD_COMMAND_WATCH_HELPER_LIMIT = 5;

export const ThreadCommandWatchCommand = TrimmedNonEmptyString.check(
  Schema.isMaxLength(THREAD_COMMAND_WATCH_MAX_COMMAND_LENGTH),
);
export const ThreadCommandWatchLabel = TrimmedNonEmptyString.check(
  Schema.isMaxLength(THREAD_COMMAND_WATCH_MAX_LABEL_LENGTH),
);
export const ThreadCommandWatchEveryMinutes = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: THREAD_COMMAND_WATCH_MAX_EVERY_MINUTES }),
);
/** How a watch compares its command's output: exact text, or JSON values. */
export const ThreadCommandWatchFormat = Schema.Literals(["text", "json"]);
export type ThreadCommandWatchFormat = typeof ThreadCommandWatchFormat.Type;

export const ThreadCommandWatchHelperPath = TrimmedNonEmptyString.check(
  Schema.isMaxLength(THREAD_COMMAND_WATCH_MAX_HELPER_PATH_LENGTH),
);

/** A script stored with the watch. The watch runs the stored copy, not the workspace file. */
export const ThreadCommandWatchHelper = Schema.Struct({
  /** Workspace-relative path the script was copied from, and its path below `$T3_WATCH_HELPERS`. */
  path: ThreadCommandWatchHelperPath,
  sha256: TrimmedNonEmptyString,
});
export type ThreadCommandWatchHelper = typeof ThreadCommandWatchHelper.Type;

export const ThreadCommandWatchStatus = Schema.Literals([
  /** Needs the user to allow it before it runs anything. */
  "awaiting_approval",
  /** Allowed, waiting for its first successful check. */
  "baselining",
  "watching",
  /** Not checking until someone resumes it. */
  "paused",
]);
export type ThreadCommandWatchStatus = typeof ThreadCommandWatchStatus.Type;

export const ThreadCommandWatchPauseReason = Schema.Literals(["failures", "storage-limit"]);
export type ThreadCommandWatchPauseReason = typeof ThreadCommandWatchPauseReason.Type;

/**
 * What lets the watch run: the thread's Full access, or the user's approval of this exact
 * revision. Full access is rechecked against the thread's current mode on every check.
 */
export const ThreadCommandWatchAuthority = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("full-access") }),
  Schema.Struct({ kind: Schema.Literal("approved"), revisionHash: TrimmedNonEmptyString }),
]);
export type ThreadCommandWatchAuthority = typeof ThreadCommandWatchAuthority.Type;

/**
 * A shell command the server runs on an interval for a thread's agent (`watch_command`). This is
 * the summary every client sees; observations and waiting changes stay in server-side watch
 * storage. Quiet checks write nothing here.
 */
export const ThreadCommandWatch = Schema.Struct({
  id: TrimmedNonEmptyString,
  command: ThreadCommandWatchCommand,
  label: ThreadCommandWatchLabel,
  everyMinutes: ThreadCommandWatchEveryMinutes,
  format: ThreadCommandWatchFormat,
  helpers: Schema.Array(ThreadCommandWatchHelper),
  /** The first check reports what exists instead of only recording it. */
  reportInitialState: Schema.Boolean,
  /** The working directory the watch was created for. */
  cwd: TrimmedNonEmptyString,
  startedAt: IsoDateTime,
  /** SHA-256 over the command, format, working directory and helper hashes. */
  revisionHash: TrimmedNonEmptyString,
  authority: Schema.NullOr(ThreadCommandWatchAuthority),
  status: ThreadCommandWatchStatus,
  pauseReason: Schema.optional(ThreadCommandWatchPauseReason),
  /**
   * The watch this revision replaces: it keeps running while this one awaits approval, and its
   * waiting changes move here when this one takes over.
   */
  replacesWatchId: Schema.optional(TrimmedNonEmptyString),
  /** Failed checks in a row, counted separately for running the command and reading its output. */
  collectionFailures: NonNegativeInt,
  comparisonFailures: NonNegativeInt,
  lastFailure: Schema.optional(Schema.String),
  /** Detected changes not yet in the thread, waiting behind `pendingMessageId`. */
  waitingChanges: NonNegativeInt,
  /** The update this watch has queued in the thread that the agent has not started on. */
  pendingMessageId: Schema.optional(MessageId),
  lastChangeAt: Schema.optional(IsoDateTime),
});
export type ThreadCommandWatch = typeof ThreadCommandWatch.Type;
