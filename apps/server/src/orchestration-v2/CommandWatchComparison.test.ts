import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { THREAD_COMMAND_WATCH_COMPARISON_SOURCE_LIMIT_BYTES } from "@t3tools/contracts";

import * as ProcessRunner from "../processRunner.ts";
import * as CommandWatchComparison from "./CommandWatchComparison.ts";

const layerTest = CommandWatchComparison.layer.pipe(
  Layer.provide(ProcessRunner.layer),
  Layer.provide(NodeServices.layer),
);

const input = { phase: "update", previous: null, current: null, state: null } as const;

/** Evaluates an expression inside the interpreter and returns what a comparison would see. */
const evaluate = (expression: string) =>
  Effect.gen(function* () {
    const comparison = yield* CommandWatchComparison.CommandWatchComparison;
    return yield* comparison.compare(
      `function compare() { return { value: (() => { ${expression} })() }; }`,
      input,
    );
  });

const failureOf = (source: string) =>
  Effect.gen(function* () {
    const comparison = yield* CommandWatchComparison.CommandWatchComparison;
    const error = yield* comparison.compare(source, input).pipe(Effect.flip);
    return error.reason;
  });

const ISSUE_COMPARISON = `
  function compare({ phase, previous, current }) {
    const own = current.viewer.toLowerCase();
    if (phase === "baseline") return { state: null, changes: [] };
    const old = new Map(previous.comments.map((comment) => [comment.id, comment]));
    const changes = [];
    for (const comment of current.comments) {
      if (comment.author.toLowerCase() === own) continue;
      const before = old.get(comment.id);
      if (before && before.body === comment.body) continue;
      changes.push({
        key: "comment:" + comment.id,
        kind: before ? "comment.edited" : "comment.added",
        summary: before ? "comment edited" : "new comment",
        details: comment,
      });
    }
    if (previous.state !== current.state) {
      changes.push({
        key: "state:" + current.state,
        kind: "issue.closed",
        summary: "issue closed",
        details: { before: previous.state, after: current.state },
      });
    }
    return { state: null, changes };
  }
`;

// These start the real helper process, so they run on the wall clock.
describe("CommandWatchComparison", () => {
  it.live("runs the issue example, and the same input gives the same result", () =>
    Effect.gen(function* () {
      const comparison = yield* CommandWatchComparison.CommandWatchComparison;
      const issue = {
        phase: "update" as const,
        previous: { viewer: "me", state: "OPEN", comments: [{ id: "1", author: "me", body: "a" }] },
        current: {
          viewer: "ME",
          state: "CLOSED",
          comments: [
            { id: "1", author: "me", body: "edited, but mine" },
            { id: "2", author: "them", body: "hello" },
          ],
        },
        state: null,
      };
      const first = yield* comparison.compare(ISSUE_COMPARISON, issue);
      assert.deepEqual(
        (JSON.parse(first) as { changes: ReadonlyArray<{ summary: string }> }).changes.map(
          (change) => change.summary,
        ),
        ["new comment", "issue closed"],
      );
      assert.strictEqual(yield* comparison.compare(ISSUE_COMPARISON, issue), first);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("leaves no route to the host, the clock, randomness or dynamic code", () =>
    Effect.gen(function* () {
      const report = yield* evaluate(`
        const attempt = (run) => { try { return String(run()); } catch (error) { return "blocked"; } };
        return {
          globals: [
            "process", "require", "module", "fetch", "console", "setTimeout", "queueMicrotask",
            "WebAssembly", "std", "os", "eval", "Function", "Proxy", "Promise", "WeakRef",
            "SharedArrayBuffer", "Atomics", "performance",
          ].filter((name) => name in globalThis),
          function: attempt(() => (function () {}).constructor("return 1")()),
          generator: attempt(() => (function* () {}).constructor("return 1")().next().value),
          async: attempt(() => (async function () {}).constructor("return 1")()),
          now: attempt(() => Date.now()),
          noArgs: attempt(() => new Date()),
          viaPrototype: attempt(() => new (new Date(0).constructor)()),
          random: attempt(() => Math.random()),
          parsed: new Date("2026-01-02T03:04:05Z").getUTCDate(),
        };
      `);
      const { value } = JSON.parse(report) as { value: Record<string, unknown> };
      assert.deepEqual(value, {
        globals: [],
        function: "blocked",
        generator: "blocked",
        async: "blocked",
        now: "blocked",
        noArgs: "blocked",
        viaPrototype: "blocked",
        random: "blocked",
        parsed: 2,
      });
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("supports ordinary synchronous JavaScript on JSON values", () =>
    Effect.gen(function* () {
      const report = yield* evaluate(`
        const seen = new Map([["a", 1]]);
        return [...seen.keys(), [...new Set([1, 1, 2])].length, "x".padStart(2, "y"), /b+/.test("abc")];
      `);
      assert.deepEqual((JSON.parse(report) as { value: unknown }).value, ["a", 2, "yx", true]);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("ends an infinite loop at its time limit without blocking the server", () =>
    Effect.gen(function* () {
      let ticks = 0;
      let turn = setImmediate(function tick() {
        ticks += 1;
        turn = setImmediate(tick);
      });
      const reason = yield* failureOf("function compare() { for (;;) {} }").pipe(
        Effect.ensuring(Effect.sync(() => clearImmediate(turn))),
      );
      assert.include(reason, "250 ms");
      // The event loop kept turning while the comparison spun.
      assert.isAbove(ticks, 5);
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("ends runaway allocation, deep recursion, a thrown error and a Promise as failures", () =>
    Effect.gen(function* () {
      assert.isNotEmpty(yield* failureOf("function compare() { let s = 'x'; for (;;) s += s; }"));
      assert.isNotEmpty(
        yield* failureOf("function f() { return f() + 1; } function compare() { return f(); }"),
      );
      assert.include(
        yield* failureOf("function compare() { throw new Error('Unexpected issue'); }"),
        "Unexpected issue",
      );
      assert.isNotEmpty(yield* failureOf("async function compare() { return {}; }"));
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("rejects values outside JSON, and an oversized result", () =>
    Effect.gen(function* () {
      for (const [expression, reason] of [
        ["return { a: NaN };", "finite"],
        ["return { a: Infinity };", "finite"],
        ["return { a: undefined };", "JSON"],
        ["return { a: 1n };", "JSON"],
        ["return { a() {} };", "JSON"],
        ["return new Map();", "plain"],
        ["const a = {}; a.self = a; return a;", "cycle"],
      ] as const) {
        assert.include(
          yield* failureOf(`function compare() { ${expression} }`),
          reason,
          expression,
        );
      }
      assert.include(
        yield* failureOf(`function compare() { return { a: "x".repeat(600000) }; }`),
        "too large",
      );
    }).pipe(Effect.provide(layerTest)),
  );

  it.live("rejects source without compare, source that does not parse, and nothing else", () =>
    Effect.gen(function* () {
      const comparison = yield* CommandWatchComparison.CommandWatchComparison;
      const reason = (source: string) =>
        comparison.compile(source).pipe(
          Effect.flip,
          Effect.map((error) => error.reason),
        );
      assert.include(yield* reason("const x = 1;"), "function named compare");
      assert.isNotEmpty(yield* reason("function ("));
      yield* comparison.compile(ISSUE_COMPARISON);
      assert.isBelow(ISSUE_COMPARISON.length, THREAD_COMMAND_WATCH_COMPARISON_SOURCE_LIMIT_BYTES);
    }).pipe(Effect.provide(layerTest)),
  );
});
