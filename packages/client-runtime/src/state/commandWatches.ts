import type { ThreadCommandWatch } from "@t3tools/contracts";

export const commandWatchIntervalLabel = (everyMinutes: number) =>
  everyMinutes % 60 === 0
    ? `every ${everyMinutes / 60 === 1 ? "hour" : `${everyMinutes / 60} hours`}`
    : `every ${everyMinutes === 1 ? "minute" : `${everyMinutes} min`}`;

export function commandWatchStatusLabel(watch: ThreadCommandWatch): string {
  switch (watch.status) {
    case "awaiting_approval":
      return "Needs your approval";
    case "baselining":
      return "Starting";
    case "paused":
      return watch.pauseReason === "storage-limit"
        ? "Paused: too many changes waiting"
        : `Paused: ${watch.lastFailure ?? "the command kept failing"}`;
    case "watching":
      return watch.waitingChanges > 0
        ? `Watching, ${watch.waitingChanges} ${watch.waitingChanges === 1 ? "change" : "changes"} waiting`
        : watch.lastFailure === undefined
          ? "Watching"
          : `Watching, last check ${watch.lastFailure}`;
  }
}

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
