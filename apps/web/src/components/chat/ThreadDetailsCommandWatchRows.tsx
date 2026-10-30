import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  AuthOrchestrationOperateScope,
  type EnvironmentId,
  type ThreadCommandWatch,
  type ThreadId,
} from "@t3tools/contracts";
import {
  COMMAND_WATCH_APPROVAL_WARNING,
  commandWatchIntervalLabel,
  commandWatchStatusLabel,
} from "@t3tools/client-runtime/state/command-watches";
import { EyeOffIcon, PlayIcon, TerminalIcon } from "lucide-react";

import { cn } from "../../lib/utils";
import { useEnvironmentScope } from "../../state/session";
import { useThreadProjection } from "../../state/entities";
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
  const projection = useThreadProjection(scopeThreadRef(props.environmentId, props.threadId));
  const watches = projection?.projection.thread.commandWatches ?? EMPTY_WATCHES;
  const stop = useAtomCommand(threadEnvironment.stopCommandWatch, { reportFailure: true });
  const approve = useAtomCommand(threadEnvironment.approveCommandWatch, { reportFailure: true });
  const resume = useAtomCommand(threadEnvironment.resumeCommandWatch, { reportFailure: true });
  const canOperateThread = useEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope);
  if (watches.length === 0) return null;

  const decide = (watch: ThreadCommandWatch, allow: boolean) =>
    void approve({
      environmentId: props.environmentId,
      input: {
        threadId: props.threadId,
        watchId: watch.id,
        revisionHash: watch.revisionHash,
        approve: allow,
      },
    });

  return (
    <ThreadDetailsSection headingId="thread-details-command-watches-heading" title="Watching">
      <ul className="m-0 list-none p-0">
        {watches.map((watch) => (
          <li
            key={watch.id}
            className={cn("rounded-lg py-1.5", THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS)}
          >
            <div className="flex items-center">
              <TerminalIcon className={THREAD_DETAILS_PANEL_ICON_CLASS} />
              <div className="min-w-0 flex-1">
                <span className="block truncate text-sm font-medium text-foreground/80">
                  {watch.label}
                </span>
                <p
                  className={cn(
                    "truncate text-2xs",
                    watch.status === "awaiting_approval" || watch.status === "paused"
                      ? "text-warning-foreground"
                      : "text-muted-foreground",
                  )}
                >
                  {commandWatchStatusLabel(watch)}
                </p>
                <Tooltip>
                  <TooltipTrigger
                    render={
                      <p className="truncate font-mono text-2xs text-muted-foreground">
                        {commandWatchIntervalLabel(watch.everyMinutes)} · {watch.command}
                      </p>
                    }
                  />
                  <TooltipPopup>{watch.command}</TooltipPopup>
                </Tooltip>
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
                        onClick={() =>
                          void resume({
                            environmentId: props.environmentId,
                            input: { threadId: props.threadId, watchId: watch.id },
                          })
                        }
                      >
                        <PlayIcon className="size-3.5" />
                      </ThreadDetailsControl>
                    }
                  />
                  <TooltipPopup>Resume watching</TooltipPopup>
                </Tooltip>
              ) : null}
              <Tooltip>
                <TooltipTrigger
                  render={
                    <ThreadDetailsControl
                      size="icon-xs"
                      variant="ghost"
                      part="icon"
                      aria-label={`Stop watching ${watch.label}`}
                      disabled={!canOperateThread}
                      onClick={() =>
                        void stop({
                          environmentId: props.environmentId,
                          input: { threadId: props.threadId, watchId: watch.id },
                        })
                      }
                    >
                      <EyeOffIcon className="size-3.5" />
                    </ThreadDetailsControl>
                  }
                />
                <TooltipPopup>Stop watching</TooltipPopup>
              </Tooltip>
            </div>
            {watch.status === "awaiting_approval" ? (
              <CommandWatchApproval
                watch={watch}
                disabled={!canOperateThread}
                onDecide={(allow) => decide(watch, allow)}
              />
            ) : null}
          </li>
        ))}
      </ul>
    </ThreadDetailsSection>
  );
}

export function CommandWatchApproval(props: {
  readonly watch: ThreadCommandWatch;
  readonly disabled: boolean;
  readonly onDecide: (allow: boolean) => void;
}) {
  const { watch } = props;
  return (
    <div className="mt-1.5 flex flex-col gap-1.5 text-2xs text-muted-foreground">
      <p>
        This command runs {commandWatchIntervalLabel(watch.everyMinutes)}{" "}
        {COMMAND_WATCH_APPROVAL_WARNING}
      </p>
      <pre className="max-h-32 overflow-auto whitespace-pre-wrap rounded-md bg-muted p-1.5 font-mono text-foreground/80">
        {watch.command}
      </pre>
      <p className="truncate font-mono">in {watch.cwd}</p>
      {watch.helpers.length > 0 ? (
        <p>Runs stored copies of {watch.helpers.map((helper) => helper.path).join(", ")}.</p>
      ) : null}
      {watch.replacesWatchId === undefined ? null : (
        <p>Replaces a running watch, which keeps running until you decide.</p>
      )}
      <div className="flex gap-1.5">
        <Button size="xs" disabled={props.disabled} onClick={() => props.onDecide(true)}>
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
