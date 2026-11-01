import {
  COMMAND_WATCH_APPROVAL_WARNING,
  COMMAND_WATCH_LIMITS_LABEL,
  commandWatchComparisonLabel,
  commandWatchIntervalLabel,
  commandWatchPurposeLabel,
  commandWatchStatusLabel,
} from "@t3tools/client-runtime/state/command-watches";
import type { EnvironmentId, ThreadCommandWatch, ThreadId } from "@t3tools/contracts";
import { useState } from "react";
import { View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { useEnvironmentServerConfig } from "../../state/entities";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useEnvironmentQuery } from "../../state/query";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";

interface CommandWatchControlsProps {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly watch: ThreadCommandWatch;
  readonly canOperateThread: boolean;
}

function useCommandWatchActions(props: CommandWatchControlsProps) {
  const approve = useAtomCommand(threadEnvironment.approveCommandWatch);
  const resume = useAtomCommand(threadEnvironment.resumeCommandWatch);
  const pause = useAtomCommand(threadEnvironment.pauseCommandWatch);
  const stop = useAtomCommand(threadEnvironment.stopCommandWatch);
  const target = { threadId: props.threadId, watchId: props.watch.id };
  return {
    decide: (allow: boolean) =>
      void approve({
        environmentId: props.environmentId,
        input: { ...target, revisionHash: props.watch.revisionHash, approve: allow },
      }),
    resume: () => void resume({ environmentId: props.environmentId, input: target }),
    pause: () => void pause({ environmentId: props.environmentId, input: target }),
    stop: () => void stop({ environmentId: props.environmentId, input: target }),
  };
}

function CodeBlock(props: { readonly text: string }) {
  return (
    <View className="rounded-xl bg-subtle p-2.5">
      <Text selectable className="font-mono text-2xs leading-normal text-foreground">
        {props.text}
      </Text>
    </View>
  );
}

/** Everything the watch runs, which is what an approval covers. */
function WatchDefinition(props: CommandWatchControlsProps) {
  const { watch } = props;
  const supportsDetails =
    useEnvironmentServerConfig(props.environmentId)?.environment.capabilities
      .commandWatchDetails === true;
  const needsDetails = watch.comparison.kind === "custom-js-v1" || watch.helpers.length > 0;
  const details = useEnvironmentQuery(
    supportsDetails && needsDetails
      ? orchestrationEnvironment.commandWatchDetails({
          environmentId: props.environmentId,
          input: { threadId: props.threadId, watchId: watch.id },
        })
      : null,
  );
  return (
    <>
      <CodeBlock text={watch.command} />
      <Text className="font-mono text-2xs text-foreground-muted" numberOfLines={2}>
        in {watch.cwd} with {watch.shell}
      </Text>
      <Text className="font-sans text-xs text-foreground-muted">
        {commandWatchPurposeLabel(watch)} {commandWatchComparisonLabel(watch)}
      </Text>
      {details.data?.comparisonSource ? <CodeBlock text={details.data.comparisonSource} /> : null}
      {watch.helpers.map((helper) => {
        const stored = details.data?.helpers.find((candidate) => candidate.path === helper.path);
        return (
          <View key={helper.path} className="gap-1">
            <Text className="font-mono text-2xs text-foreground-muted" numberOfLines={1}>
              Stored copy of {helper.path}
            </Text>
            {stored === undefined ? null : <CodeBlock text={stored.content} />}
          </View>
        );
      })}
      <Text className="font-sans text-xs text-foreground-muted">{COMMAND_WATCH_LIMITS_LABEL}</Text>
    </>
  );
}

function ApprovalDetails(props: CommandWatchControlsProps) {
  const { watch } = props;
  return (
    <>
      <Text className="font-sans text-sm leading-normal text-foreground-secondary">
        This command runs {commandWatchIntervalLabel(watch.everyMinutes)}{" "}
        {COMMAND_WATCH_APPROVAL_WARNING}
      </Text>
      <WatchDefinition {...props} />
      {watch.replacesWatchId === undefined ? null : (
        <Text className="font-sans text-xs text-foreground-muted">
          Replaces a running watch, which keeps running until you decide.
        </Text>
      )}
    </>
  );
}

/** Above the composer while a watch waits for the user's approval. */
export function CommandWatchApprovalCard(props: CommandWatchControlsProps) {
  const actions = useCommandWatchActions(props);
  return (
    <View className="gap-2.5 rounded-[20px] border border-border bg-card-alt p-4">
      <Text className="font-t3-bold text-2xs uppercase tracking-[1.1px] text-foreground-secondary">
        Allow this watch?
      </Text>
      <Text className="font-t3-bold text-lg text-foreground">{props.watch.label}</Text>
      <ApprovalDetails {...props} />
      <View className="flex-row flex-wrap gap-2.5">
        <Button
          label="Allow this watch"
          tone="primary"
          disabled={!props.canOperateThread}
          onPress={() => actions.decide(true)}
        />
        <Button
          label="Deny"
          tone="danger"
          disabled={!props.canOperateThread}
          onPress={() => actions.decide(false)}
        />
      </View>
      {!props.canOperateThread ? (
        <Text className="font-sans text-xs text-foreground-muted">
          This connection cannot approve watches.
        </Text>
      ) : null}
    </View>
  );
}

/** One watch in the background work sheet, with its way out. */
export function CommandWatchRow(props: CommandWatchControlsProps & { readonly settled: boolean }) {
  const { watch } = props;
  const actions = useCommandWatchActions(props);
  const needsUser = watch.status === "awaiting_approval" || watch.status === "paused";
  const [showDefinition, setShowDefinition] = useState(false);
  return (
    <View className="gap-2 border-b border-border py-3.5">
      <View className="min-w-0">
        <Text className="font-t3-medium text-sm text-foreground">{watch.label}</Text>
        <Text
          className={
            needsUser ? "text-xs text-warning-foreground" : "text-xs text-foreground-muted"
          }
        >
          {commandWatchStatusLabel(watch, props.settled)}
        </Text>
        {watch.status === "awaiting_approval" ? null : (
          <Text className="font-mono text-2xs text-foreground-muted" numberOfLines={2}>
            {commandWatchIntervalLabel(watch.everyMinutes)} · {watch.command}
          </Text>
        )}
      </View>
      {watch.status === "awaiting_approval" ? (
        <ApprovalDetails {...props} />
      ) : (
        <>
          <Button
            label={showDefinition ? "Hide details" : "Show details"}
            onPress={() => setShowDefinition((shown) => !shown)}
          />
          {showDefinition ? <WatchDefinition {...props} /> : null}
        </>
      )}
      <View className="flex-row flex-wrap gap-2.5">
        {watch.status === "awaiting_approval" ? (
          <>
            <Button
              label="Allow this watch"
              tone="primary"
              disabled={!props.canOperateThread}
              onPress={() => actions.decide(true)}
            />
            <Button
              label="Deny"
              tone="danger"
              disabled={!props.canOperateThread}
              onPress={() => actions.decide(false)}
            />
          </>
        ) : watch.status === "ended" ? null : (
          <>
            {watch.status === "paused" ? (
              <Button
                label="Resume"
                tone="primary"
                disabled={!props.canOperateThread}
                onPress={actions.resume}
              />
            ) : (
              <Button label="Pause" disabled={!props.canOperateThread} onPress={actions.pause} />
            )}
            <Button
              label="Stop watching"
              disabled={!props.canOperateThread}
              onPress={actions.stop}
            />
          </>
        )}
      </View>
    </View>
  );
}
