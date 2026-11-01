import type { ThreadCommandWatch } from "@t3tools/contracts";

export const commandWatchIntervalLabel = (everyMinutes: number) =>
  everyMinutes % 60 === 0
    ? `every ${everyMinutes / 60 === 1 ? "hour" : `${everyMinutes / 60} hours`}`
    : `every ${everyMinutes === 1 ? "minute" : `${everyMinutes} min`}`;

export function commandWatchStatusLabel(watch: ThreadCommandWatch, settled = false): string {
  if (settled && isCommandWatchRunning(watch)) return "Paused while the thread is settled";
  switch (watch.status) {
    case "awaiting_approval":
      return "Needs your approval";
    case "baselining":
      // A failed first check must not read as watching: there is nothing to compare with yet.
      return watch.lastFailure === undefined
        ? "Starting"
        : `Not started: last check ${watch.lastFailure}`;
    case "ended":
      return watch.endReason === "completed"
        ? `Completed${watch.endDetail === undefined ? "" : `: ${watch.endDetail}`}`
        : watch.endReason === "denied"
          ? "Denied"
          : "Stopped";
    case "paused":
      switch (watch.pauseReason) {
        case "user":
          return "Paused by you";
        case "agent":
          return "Paused by the agent";
        case "storage-limit":
          return "Paused: too many changes waiting";
        default:
          return `Paused: ${watch.lastFailure ?? "the command kept failing"}`;
      }
    case "watching":
      return watch.waitingChanges > 0
        ? `Watching, ${watch.waitingChanges} ${watch.waitingChanges === 1 ? "change" : "changes"} waiting`
        : watch.lastFailure === undefined
          ? "Watching"
          : `Watching, last check ${watch.lastFailure}`;
  }
}

export const isCommandWatchRunning = (watch: ThreadCommandWatch) =>
  watch.status === "watching" || watch.status === "baselining";

/** One line naming a thread's watches. */
export function commandWatchesTitle(watches: ReadonlyArray<ThreadCommandWatch>): string {
  const [only] = watches;
  if (watches.length !== 1 || only === undefined) return `Watching ${watches.length} commands`;
  switch (only.status) {
    case "awaiting_approval":
      return `Needs approval: ${only.label}`;
    case "baselining":
      return `Starting watch: ${only.label}`;
    case "paused":
      return `Paused: ${only.label}`;
    case "watching":
      return `Watching: ${only.label}`;
    case "ended":
      return `Ended: ${only.label}`;
  }
}

/** Ended watches stay listed for a while, but are no longer anything to act on. */
export const commandWatchesLive = (watches: ReadonlyArray<ThreadCommandWatch>) =>
  watches.filter((watch) => watch.status !== "ended");

export function commandWatchComparisonLabel(watch: ThreadCommandWatch): string {
  switch (watch.comparison.kind) {
    case "text-v1":
      return "Any change to the output is news.";
    case "json-v1":
      return "Any change to the JSON value is news, ignoring formatting and key order.";
    case "custom-js-v1":
      return "A script the agent wrote decides what is news. It cannot reach files, the network or other programs.";
  }
}

export function commandWatchPurposeLabel(watch: ThreadCommandWatch): string {
  return watch.completion.kind === "ongoing"
    ? "Runs until stopped."
    : `Ends by itself when: ${watch.completion.description}`;
}

export const COMMAND_WATCH_LIMITS_LABEL =
  "Each check may run for 60 seconds and print 4 KB. It pauses after 8 failed checks in a row, or when 1 MB of changes are waiting.";

export const COMMAND_WATCH_APPROVAL_WARNING =
  "outside the agent's sandbox, with the T3 server's account permissions, until the watch is stopped.";

/** The watch that needs the user first: approval before a paused watch. */
export function commandWatchNeedingUser(
  watches: ReadonlyArray<ThreadCommandWatch>,
): ThreadCommandWatch | null {
  return (
    watches.find((watch) => watch.status === "awaiting_approval") ??
    watches.find((watch) => watch.status === "paused") ??
    null
  );
}
