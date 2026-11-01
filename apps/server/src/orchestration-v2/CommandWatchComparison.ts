import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeURL from "node:url";

import * as ProcessRunner from "../processRunner.ts";
import type { ComparisonWorkerRequest } from "./commandWatchComparisonWorker.ts";

/**
 * Includes starting the helper process, so it only catches what the interpreter's own limits
 * cannot: native work such as a huge allocation does not reach the interrupt check.
 */
const HARD_TIMEOUT_SECONDS = 10;
/** Result bytes one comparison may return. Longer results are failures, never cut. */
const COMMAND_WATCH_COMPARISON_RESULT_LIMIT_BYTES = 256 * 1024;

export class CommandWatchComparisonError extends Schema.TaggedError<CommandWatchComparisonError>()(
  "CommandWatchComparisonError",
  {
    reason: Schema.String,
    /** The runtime itself could not run, as opposed to the comparison failing. */
    unavailable: Schema.Boolean,
  },
) {
  override get message(): string {
    return this.reason;
  }
}

export interface CommandWatchComparisonInput {
  readonly phase: "baseline" | "update";
  readonly previous: unknown;
  readonly current: unknown;
  readonly state: unknown;
}

const WorkerRequestJson = Schema.fromJsonString(Schema.Unknown);
const WorkerResponseJson = Schema.fromJsonString(
  Schema.Union([
    Schema.Struct({ ok: Schema.Literal(true), result: Schema.NullOr(Schema.String) }),
    Schema.Struct({ ok: Schema.Literal(false), error: Schema.String }),
  ]),
);
const encodeRequest = Schema.encodeEffect(WorkerRequestJson);
const decodeResponse = Schema.decodeEffect(WorkerResponseJson);

/**
 * Runs agent-written comparison in a disposable helper process holding a restricted interpreter.
 * The interpreter removes capabilities; the process keeps a runaway or crashing comparison from
 * blocking or taking down the server. There is no unrestricted fallback.
 */
export class CommandWatchComparison extends Context.Service<
  CommandWatchComparison,
  {
    /** Rejects source that does not compile or does not define `compare`. */
    readonly compile: (source: string) => Effect.Effect<void, CommandWatchComparisonError>;
    /** The result as JSON text, not yet checked against the result contract. */
    readonly compare: (
      source: string,
      input: CommandWatchComparisonInput,
    ) => Effect.Effect<string, CommandWatchComparisonError>;
  }
>()("t3/orchestration-v2/CommandWatchComparison") {}

const make = Effect.gen(function* () {
  const processes = yield* ProcessRunner.ProcessRunner;
  const executable = yield* HostProcess.ExecutablePath;
  const isExecutable = yield* HostProcess.IsExecutable;
  // In a bundle import.meta.url is bin.mjs; development and tests use this source module.
  const entry = NodeURL.fileURLToPath(
    new URL(import.meta.url.endsWith(".ts") ? "../bin.ts" : "./bin.mjs", import.meta.url),
  );

  const run = Effect.fn("CommandWatchComparison.run")(function* (request: ComparisonWorkerRequest) {
    const failure = (reason: string, unavailable = false) =>
      new CommandWatchComparisonError({ reason, unavailable });
    const stdin = yield* encodeRequest(request).pipe(
      Effect.mapError(() => failure("was given sample data that is not JSON")),
    );
    const output = yield* processes
      .run({
        command: executable,
        args: [...(isExecutable ? [] : [entry]), "__watch-compare"],
        stdin,
        // Dates read in UTC on every machine, so the same input gives the same result.
        env: { ELECTRON_RUN_AS_NODE: "1", TZ: "UTC" },
        timeout: `${HARD_TIMEOUT_SECONDS} seconds`,
        timeoutBehavior: "timedOutResult",
        maxOutputBytes: COMMAND_WATCH_COMPARISON_RESULT_LIMIT_BYTES * 2,
        outputMode: "truncate",
      })
      .pipe(
        Effect.mapError(
          (error) =>
            new CommandWatchComparisonError({
              reason: `the comparison runtime could not start: ${error.message}`,
              unavailable: true,
            }),
        ),
      );
    if (output.timedOut) return yield* failure("exceeded its time or memory limit");
    if (output.stdoutTruncated) return yield* failure("returned a result that is too large");
    // A crash in the interpreter, such as exhausting its stack, ends only this process.
    if (output.code !== 0) return yield* failure("exceeded its stack or memory limit");
    const response = yield* decodeResponse(output.stdout).pipe(
      Effect.mapError(() => failure("the comparison runtime returned no result", true)),
    );
    if (!response.ok) return yield* failure(response.error);
    return response.result;
  });

  return CommandWatchComparison.of({
    compile: Effect.fn("CommandWatchComparison.compile")(function* (source) {
      yield* run({ operation: "compile", source });
    }),
    compare: Effect.fn("CommandWatchComparison.compare")(function* (source, input) {
      return (yield* run({ operation: "compare", source, input })) ?? "null";
    }),
  });
});

export const layer = Layer.effect(CommandWatchComparison, make);
