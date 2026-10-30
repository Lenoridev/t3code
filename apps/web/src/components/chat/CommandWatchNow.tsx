import {
  commandWatchNeedingUser,
  commandWatchStatusLabel,
} from "@t3tools/client-runtime/state/command-watches";
import {
  AuthOrchestrationOperateScope,
  type EnvironmentId,
  type ThreadCommandWatch,
  type ThreadId,
} from "@t3tools/contracts";
import { CirclePauseIcon, ShieldQuestionIcon } from "lucide-react";
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
