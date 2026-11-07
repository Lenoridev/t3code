import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  AuthOrchestrationOperateScope,
  type EnvironmentId,
  type ThreadCommandWatch,
  type ThreadId,
} from "@t3tools/contracts";
import {
  COMMAND_WATCH_APPROVAL_WARNING,
  commandWatchComparisonLabel,
  commandWatchHasWork,
  commandWatchLimitsLabel,
  commandWatchNeedsDetails,
  commandWatchesLive,
  commandWatchIntervalLabel,
  commandWatchPurposeLabel,
  commandWatchStatusLabel,
  isCommandWatchRunning,
} from "@t3tools/client-runtime/state/command-watches";
import { commandWatchesSuspended } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { ChevronDown, ChevronUp } from "lucide";
import { CirclePauseIcon, PauseIcon, PlayIcon, SquareIcon, TerminalIcon } from "lucide-react";
import { useState } from "react";

import { MorphIcon } from "~/components/MorphIcon";
import { cn } from "../../lib/utils";
import { formatRelativeTimeLabel } from "../../timestampFormat";
import { useEnvironmentScope } from "../../state/session";
import { useQuietThreadProjection, useServerConfigs } from "../../state/entities";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useEnvironmentQuery } from "../../state/query";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { ThreadDetailsControl } from "./ThreadDetailsControl";
import { ThreadDetailsSection } from "./ThreadDetailsSection";
import {
  THREAD_DETAILS_PANEL_ICON_CLASS,
  THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS,
} from "./threadDetailsPanelStyles";

const EMPTY_WATCHES: ReadonlyArray<ThreadCommandWatch> = [];

/**
 * Thread details section listing the commands the server runs for this thread's agent
 * (`watch_command`), each with its way out: approve or deny, resume, and stop.
 * Renders nothing without watches.
 */
export function ThreadDetailsCommandWatchRows(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const thread = useQuietThreadProjection(scopeThreadRef(props.environmentId, props.threadId))
    ?.projection.thread;
  // Ended watches have nothing left to act on or inspect; agents still list them.
  const watches = commandWatchesLive(thread?.commandWatches ?? EMPTY_WATCHES);
  const settled = thread !== undefined && commandWatchesSuspended(thread);
  if (watches.length === 0) return null;

  return (
    <ThreadDetailsSection headingId="thread-details-command-watches-heading" title="Watching">
      <ul data-thread-details-watch-list className="m-0 list-none p-0">
        {watches.map((watch) => (
          <CommandWatchRow
            key={watch.id}
            settled={settled}
            environmentId={props.environmentId}
            threadId={props.threadId}
            watch={watch}
          />
        ))}
      </ul>
    </ThreadDetailsSection>
  );
}

function CommandWatchRow(props: {
  readonly settled: boolean;
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watch: ThreadCommandWatch;
}) {
  const { watch } = props;
  const [expanded, setExpanded] = useState(false);
  const stop = useAtomCommand(threadEnvironment.stopCommandWatch, { reportFailure: true });
  const approve = useAtomCommand(threadEnvironment.approveCommandWatch, { reportFailure: true });
  const resume = useAtomCommand(threadEnvironment.resumeCommandWatch, { reportFailure: true });
  const pause = useAtomCommand(threadEnvironment.pauseCommandWatch, { reportFailure: true });
  const canOperateThread = useEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope);
  const target = { threadId: props.threadId, watchId: watch.id };
  const stopLabel = watch.status === "ended" ? "Discard the last update of" : "Stop watching";
  const decide = (allow: boolean) =>
    void approve({
      environmentId: props.environmentId,
      input: { ...target, revisionHash: watch.revisionHash, approve: allow },
    });

  return (
    <li className={cn("rounded-lg py-1.5", THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS)}>
      <div className="flex items-center gap-2.5">
        {watch.status === "paused" ? (
          <CirclePauseIcon
            className={cn(THREAD_DETAILS_PANEL_ICON_CLASS, "text-warning-foreground")}
          />
        ) : (
          <TerminalIcon className={THREAD_DETAILS_PANEL_ICON_CLASS} />
        )}
        <div className="min-w-0 flex-1">
          <span className="block truncate text-sm font-medium text-foreground/80">
            {watch.label}
          </span>
          <p
            className={cn(
              "truncate text-2xs",
              watch.status === "awaiting_approval" ||
                watch.status === "paused" ||
                watch.lastFailure !== undefined
                ? "text-warning-foreground"
                : "text-muted-foreground",
            )}
          >
            {commandWatchStatusLabel(watch, props.settled)}
          </p>
          <p className="truncate font-mono text-2xs text-muted-foreground">
            {commandWatchIntervalLabel(watch.everyMinutes)} · {watch.command}
          </p>
        </div>
        {watch.status === "paused" ? (
          <Tooltip>
            <TooltipTrigger
              render={
                <ThreadDetailsControl
                  size="icon-xs"
                  variant="ghost"
                  part="icon"
                  aria-label={`Resume watching ${watch.label}`}
                  disabled={!canOperateThread}
                  onClick={() => void resume({ environmentId: props.environmentId, input: target })}
                >
                  <PlayIcon className="size-3.5" />
                </ThreadDetailsControl>
              }
            />
            <TooltipPopup>Resume watching</TooltipPopup>
          </Tooltip>
        ) : isCommandWatchRunning(watch) ? (
          <Tooltip>
            <TooltipTrigger
              render={
                <ThreadDetailsControl
                  size="icon-xs"
                  variant="ghost"
                  part="icon"
                  aria-label={`Pause watching ${watch.label}`}
                  disabled={!canOperateThread}
                  onClick={() => void pause({ environmentId: props.environmentId, input: target })}
                >
                  <PauseIcon className="size-3.5" />
                </ThreadDetailsControl>
              }
            />
            <TooltipPopup>Pause watching</TooltipPopup>
          </Tooltip>
        ) : null}
        {!commandWatchHasWork(watch) ? null : (
          <Tooltip>
            <TooltipTrigger
              render={
                <ThreadDetailsControl
                  size="icon-xs"
                  variant="ghost"
                  part="icon"
                  tone="destructive"
                  aria-label={`${stopLabel} ${watch.label}`}
                  disabled={!canOperateThread}
                  onClick={() => void stop({ environmentId: props.environmentId, input: target })}
                >
                  <SquareIcon aria-hidden className="size-3 fill-current" />
                </ThreadDetailsControl>
              }
            />
            <TooltipPopup>
              {watch.status === "ended" ? "Discard last update" : "Stop watching"}
            </TooltipPopup>
          </Tooltip>
        )}
      </div>
      {watch.status === "awaiting_approval" ? (
        <CommandWatchApproval
          environmentId={props.environmentId}
          threadId={props.threadId}
          watch={watch}
          disabled={!canOperateThread}
          onDecide={decide}
        />
      ) : (
        <>
          <ThreadDetailsControl
            size="sm"
            part="row"
            tone="muted"
            className="w-full active:scale-100"
            aria-expanded={expanded}
            onClick={() => setExpanded(!expanded)}
          >
            <MorphIcon
              aria-hidden
              className="size-4 shrink-0"
              icon={expanded ? ChevronUp : ChevronDown}
            />
            {expanded ? "Hide details" : "Show details"}
          </ThreadDetailsControl>
          {expanded ? (
            <>
              <p className="mt-1.5 text-2xs text-muted-foreground">
                {watch.lastChangeAt === undefined
                  ? "No changes reported yet."
                  : `Last change ${formatRelativeTimeLabel(watch.lastChangeAt)}.`}
                {watch.waitingChanges > 0 ? ` ${watch.waitingChanges} waiting.` : ""}
              </p>
              {watch.lastFailure === undefined ? null : (
                <p className="mt-1.5 text-2xs break-words text-warning-foreground">
                  Last check {watch.lastFailure}
                </p>
              )}
              <CommandWatchDefinition
                environmentId={props.environmentId}
                threadId={props.threadId}
                watch={watch}
              />
            </>
          ) : null}
        </>
      )}
    </li>
  );
}

/** The definition text a projection leaves out, loaded only for watches that have any. */
function useCommandWatchDetails(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watch: ThreadCommandWatch;
}) {
  const supportsDetails =
    useServerConfigs().get(props.environmentId)?.environment.capabilities.commandWatchDetails ===
    true;
  const needsDetails = commandWatchNeedsDetails(props.watch);
  const details = useEnvironmentQuery(
    supportsDetails && needsDetails
      ? orchestrationEnvironment.commandWatchDetails({
          environmentId: props.environmentId,
          input: { threadId: props.threadId, watchId: props.watch.id },
        })
      : null,
  );
  // Approval covers code the user must be able to read first.
  return { details, reviewable: !needsDetails || details.data != null };
}

/** Everything the watch runs, which is what an approval covers. */
function CommandWatchDefinition(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watch: ThreadCommandWatch;
}) {
  const { watch } = props;
  const { details } = useCommandWatchDetails(props);
  const code =
    "max-h-32 overflow-auto whitespace-pre-wrap rounded-md bg-muted p-1.5 font-mono text-foreground/80";
  return (
    <div className="mt-1.5 flex flex-col gap-1.5 text-2xs text-muted-foreground">
      <p className="font-medium text-foreground/80">Command</p>
      <pre className={code}>{watch.command}</pre>
      <p className="truncate font-mono">
        in {watch.cwd} with {watch.shell}
      </p>
      <p>
        Runs {commandWatchIntervalLabel(watch.everyMinutes)}. {commandWatchPurposeLabel(watch)}
      </p>
      <p>{commandWatchComparisonLabel(watch)}</p>
      {details.data?.comparisonSource ? (
        <>
          <p className="font-medium text-foreground/80">Comparison script</p>
          <pre className={code}>{details.data.comparisonSource}</pre>
        </>
      ) : null}
      {watch.helpers.map((helper) => {
        const stored = details.data?.helpers.find((candidate) => candidate.path === helper.path);
        return (
          <div key={helper.path} className="flex flex-col gap-0.5">
            <p className="truncate font-mono">Stored copy of {helper.path}</p>
            {stored === undefined ? null : <pre className={code}>{stored.content}</pre>}
          </div>
        );
      })}
      {details.error === null ? null : <p>Could not load the full definition.</p>}
      <p>{commandWatchLimitsLabel(watch)}</p>
    </div>
  );
}

export function CommandWatchApproval(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watch: ThreadCommandWatch;
  readonly disabled: boolean;
  readonly onDecide: (allow: boolean) => void;
}) {
  const { watch } = props;
  const { reviewable } = useCommandWatchDetails(props);
  return (
    <div className="mt-1.5 flex flex-col gap-1.5 text-2xs text-muted-foreground">
      <p>
        This command runs {commandWatchIntervalLabel(watch.everyMinutes)}{" "}
        {COMMAND_WATCH_APPROVAL_WARNING}
      </p>
      <CommandWatchDefinition
        environmentId={props.environmentId}
        threadId={props.threadId}
        watch={watch}
      />
      {watch.replacesWatchId === undefined ? null : (
        <p>Replaces a running watch, which keeps running until you decide.</p>
      )}
      <div className="flex gap-1.5">
        <Button
          size="xs"
          disabled={props.disabled || !reviewable}
          onClick={() => props.onDecide(true)}
        >
          Allow this watch
        </Button>
        <Button
          size="xs"
          variant="outline"
          disabled={props.disabled}
          onClick={() => props.onDecide(false)}
        >
          Deny
        </Button>
      </div>
    </div>
  );
}
