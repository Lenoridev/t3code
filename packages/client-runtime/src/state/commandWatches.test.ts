import { ThreadCommandWatch } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { commandWatchesTitle } from "./commandWatches.ts";

const decodeWatch = Schema.decodeSync(ThreadCommandWatch);
const watch = (label: string, status: ThreadCommandWatch["status"]) =>
  decodeWatch({
    id: `watch-${label}`,
    command: "cat status.txt",
    label,
    everyMinutes: 1,
    format: "text",
    helpers: [],
    reportInitialState: false,
    cwd: "/work/app",
    shell: "/bin/sh",
    comparison: { kind: "text-v1" },
    completion: { kind: "ongoing" },
    clientRequestId: `request-${label}`,
    startedAt: "2026-11-19T00:00:00.000Z",
    revisionHash: "hash",
    authority: null,
    status,
    ...(status === "paused" ? { pauseReason: "user" } : {}),
    collectionFailures: 0,
    comparisonFailures: 0,
    waitingChanges: 0,
  });

describe("commandWatchesTitle", () => {
  it("says what several watches need, not only that they watch", () => {
    expect(commandWatchesTitle([watch("a", "watching"), watch("b", "watching")])).toBe(
      "Watching 2 commands",
    );
    expect(
      commandWatchesTitle([watch("a", "awaiting_approval"), watch("b", "awaiting_approval")]),
    ).toBe("2 watches: 2 need approval");
    expect(
      commandWatchesTitle([
        watch("a", "watching"),
        watch("b", "awaiting_approval"),
        watch("c", "paused"),
      ]),
    ).toBe("3 watches: 1 needs approval, 1 paused");
  });
});
