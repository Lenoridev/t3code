import {
  commandWatchesTitle,
  commandWatchIntervalLabel,
  commandWatchNeedingUser,
  commandWatchStatusLabel,
  isCommandWatchRunning,
} from "@t3tools/client-runtime/state/command-watches";
import {
  AuthOrchestrationOperateScope,
  type EnvironmentId,
  type ThreadCommandWatch,
  type ThreadId,
} from "@t3tools/contracts";
import { CirclePauseIcon, EyeIcon, ShieldQuestionIcon } from "lucide-react";
import { useState } from "react";

import { useEnvironmentScope } from "../../state/session";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { InlineButton } from "../ui/button";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import { CommandWatchApproval } from "./ThreadDetailsCommandWatchRows";
import type { ThreadNowItem } from "./ThreadNow";

/** A command watch waiting for approval or a resume, for the end of the timeline. */
export function commandWatchNowItem(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watches: ReadonlyArray<ThreadCommandWatch> | undefined;
}): ThreadNowItem | null {
  const watch = commandWatchNeedingUser(input.watches ?? []);
  if (watch === null) return null;
  const awaitingApproval = watch.status === "awaiting_approval";
  return {
    id: `command-watch:${watch.id}`,
    icon: awaitingApproval ? (
      <ShieldQuestionIcon className="text-info-foreground" />
    ) : (
      <CirclePauseIcon className="text-warning-foreground" />
    ),
    title: awaitingApproval ? `Allow watch: ${watch.label}?` : `Watch paused: ${watch.label}`,
    description: awaitingApproval ? watch.command : commandWatchStatusLabel(watch),
    actions: (
      <CommandWatchNowActions
        environmentId={input.environmentId}
        threadId={input.threadId}
        watch={watch}
      />
    ),
  };
}

function runningWatchDetail(watch: ThreadCommandWatch): string {
  const waiting =
    watch.waitingChanges > 0
      ? `, ${watch.waitingChanges} ${watch.waitingChanges === 1 ? "change" : "changes"} waiting`
      : "";
  const failure = watch.lastFailure === undefined ? "" : `, last check ${watch.lastFailure}`;
  return `${commandWatchIntervalLabel(watch.everyMinutes)}${waiting}${failure}`;
}

/** The watches running for this thread, which stay on screen while the agent works. */
export function commandWatchRunningNowItem(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watches: ReadonlyArray<ThreadCommandWatch> | undefined;
  readonly settled: boolean;
}): ThreadNowItem | null {
  const running = (input.watches ?? []).filter(isCommandWatchRunning);
  const [only] = running;
  if (only === undefined) return null;
  return {
    id: `command-watches:${input.threadId}`,
    icon: <EyeIcon />,
    title: input.settled
      ? `Paused while settled: ${running.map((watch) => watch.label).join(", ")}`
      : commandWatchesTitle(running),
    description: input.settled
      ? "Checks restart when you un-settle this thread."
      : running.length === 1
        ? runningWatchDetail(only)
        : running.map((watch) => watch.label).join(", "),
    actions: (
      <RunningWatchActions
        environmentId={input.environmentId}
        threadId={input.threadId}
        watches={running}
      />
    ),
  };
}

function RunningWatchActions(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watches: ReadonlyArray<ThreadCommandWatch>;
}) {
  const stop = useAtomCommand(threadEnvironment.stopCommandWatch, { reportFailure: true });
  const canOperate = useEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope);
  const stopButton = (watch: ThreadCommandWatch) => (
    <InlineButton
      tone="muted"
      disabled={!canOperate}
      aria-label={`Stop watching ${watch.label}`}
      onClick={() =>
        void stop({
          environmentId: props.environmentId,
          input: { threadId: props.threadId, watchId: watch.id },
        })
      }
    >
      Stop
    </InlineButton>
  );
  const [only] = props.watches;
  if (props.watches.length === 1 && only !== undefined) return stopButton(only);
  return (
    <Popover>
      <PopoverTrigger render={<InlineButton tone="muted" />}>Manage</PopoverTrigger>
      <PopoverPopup side="top" align="end" width="lg" aria-label="Running watches">
        <ul className="flex flex-col gap-3">
          {props.watches.map((watch) => (
            <li key={watch.id} className="flex min-w-0 items-baseline gap-3 text-xs">
              <span className="min-w-0 flex-1">
                <span className="block truncate font-medium">{watch.label}</span>
                <span className="block truncate text-muted-foreground">
                  {runningWatchDetail(watch)}
                </span>
              </span>
              {stopButton(watch)}
            </li>
          ))}
        </ul>
      </PopoverPopup>
    </Popover>
  );
}

function CommandWatchNowActions(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watch: ThreadCommandWatch;
}) {
  const { watch } = props;
  const [reviewOpen, setReviewOpen] = useState(false);
  const approve = useAtomCommand(threadEnvironment.approveCommandWatch, { reportFailure: true });
  const resume = useAtomCommand(threadEnvironment.resumeCommandWatch, { reportFailure: true });
  const stop = useAtomCommand(threadEnvironment.stopCommandWatch, { reportFailure: true });
  const canOperate = useEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope);
  const target = { threadId: props.threadId, watchId: watch.id };
  const stopButton = (
    <InlineButton
      tone="muted"
      disabled={!canOperate}
      onClick={() => void stop({ environmentId: props.environmentId, input: target })}
    >
      Stop
    </InlineButton>
  );
  if (watch.status === "paused") {
    return (
      <>
        <InlineButton
          disabled={!canOperate}
          onClick={() => void resume({ environmentId: props.environmentId, input: target })}
        >
          Resume
        </InlineButton>
        {stopButton}
      </>
    );
  }
  // Consent needs the full command and its warning, which the one-line summary cuts.
  return (
    <Popover open={reviewOpen} onOpenChange={setReviewOpen}>
      <PopoverTrigger render={<InlineButton disabled={!canOperate} />}>Review</PopoverTrigger>
      <PopoverPopup side="top" align="end" width="lg" aria-label={`Review watch ${watch.label}`}>
        <CommandWatchApproval
          environmentId={props.environmentId}
          threadId={props.threadId}
          watch={watch}
          disabled={!canOperate}
          onDecide={(allow) => {
            setReviewOpen(false);
            void approve({
              environmentId: props.environmentId,
              input: { ...target, revisionHash: watch.revisionHash, approve: allow },
            });
          }}
        />
      </PopoverPopup>
    </Popover>
  );
}
