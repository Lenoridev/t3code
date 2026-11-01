import type { QuickJSContext, QuickJSHandle } from "quickjs-emscripten-core";
import { newQuickJSWASMModuleFromVariant } from "quickjs-emscripten-core";
import * as variantModule from "@jitl/quickjs-singlefile-cjs-release-sync";

/** Measured from after the interpreter loads, so a slow machine's startup does not count. */
const COMPARISON_TIME_LIMIT_MS = 250;
const COMPARISON_MEMORY_LIMIT_BYTES = 32 * 1024 * 1024;
// Above this, deep recursion overflows the WebAssembly stack before QuickJS's own check fires.
const COMPARISON_STACK_LIMIT_BYTES = 256 * 1024;
const ERROR_LIMIT = 500;

/**
 * Runs before the comparison. It leaves only pure JavaScript: every global outside the list goes,
 * including eval and Function, and each function kind loses its route to the code constructors.
 * Its closure holds what `run` needs, so a comparison cannot replace JSON under it.
 */
const PRELUDE = `(() => {
  const allowed = new Set(["Error","EvalError","RangeError","ReferenceError","SyntaxError",
    "TypeError","URIError","AggregateError","Array","Object","Iterator","parseInt","parseFloat",
    "isNaN","isFinite","decodeURI","decodeURIComponent","encodeURI","encodeURIComponent",
    "Infinity","NaN","undefined","Number","Boolean","String","Math","Reflect","Symbol",
    "globalThis","BigInt","RegExp","JSON","Map","Set","WeakMap","WeakSet","Date"]);
  for (const name of Reflect.ownKeys(globalThis)) {
    if (!allowed.has(name)) Reflect.deleteProperty(globalThis, name);
  }
  const samples = [function () {}, function* () {}, async function () {}, async function* () {}];
  for (const sample of samples) {
    Object.defineProperty(Object.getPrototypeOf(sample), "constructor", { value: undefined });
  }
  Reflect.deleteProperty(Math, "random");
  const RealDate = Date;
  function ParsedDate(...args) {
    if (!new.target || args.length === 0) throw new TypeError("A comparison has no current time");
    return new RealDate(...args);
  }
  ParsedDate.UTC = RealDate.UTC;
  ParsedDate.parse = RealDate.parse;
  ParsedDate.prototype = RealDate.prototype;
  Object.defineProperty(RealDate.prototype, "constructor", { value: ParsedDate });
  globalThis.Date = ParsedDate;

  const { parse, stringify } = JSON;
  const { isArray } = Array;
  const { getPrototypeOf, prototype: objectPrototype } = Object;
  const { isFinite: finite } = Number;
  const { ownKeys } = Reflect;
  const check = (value, path, seen) => {
    if (value === null || typeof value === "string" || typeof value === "boolean") return;
    if (typeof value === "number") {
      if (!finite(value)) throw new TypeError(path + " is not a finite number");
      return;
    }
    if (typeof value !== "object") throw new TypeError(path + " is not a JSON value");
    if (seen.has(value)) throw new TypeError(path + " is part of a cycle");
    seen.add(value);
    if (isArray(value)) {
      for (let index = 0; index < value.length; index += 1) {
        check(value[index], path + "[" + index + "]", seen);
      }
    } else {
      const prototype = getPrototypeOf(value);
      if (prototype !== objectPrototype && prototype !== null) {
        throw new TypeError(path + " is not a plain object");
      }
      for (const key of ownKeys(value)) {
        if (typeof key !== "string") throw new TypeError(path + " has a symbol key");
        check(value[key], path + "." + key, seen);
      }
    }
    seen.delete(value);
  };
  return (compare, input) => {
    const result = compare(parse(input));
    check(result, "result", new Set());
    return stringify(result);
  };
})()`;

export type ComparisonWorkerRequest =
  | { readonly operation: "compile"; readonly source: string }
  | { readonly operation: "compare"; readonly source: string; readonly input: unknown };

export type ComparisonWorkerResponse =
  | { readonly ok: true; readonly result: string | null }
  | { readonly ok: false; readonly error: string };

function describeError(context: QuickJSContext, handle: QuickJSHandle): string {
  const dumped: unknown = context.dump(handle);
  const text =
    typeof dumped === "object" && dumped !== null && "message" in dumped
      ? String(dumped.message)
      : String(dumped);
  if (text === "interrupted") return `took longer than ${COMPARISON_TIME_LIMIT_MS} ms`;
  return text.slice(0, ERROR_LIMIT);
}

async function runComparison(request: ComparisonWorkerRequest): Promise<ComparisonWorkerResponse> {
  // A single file holding the WebAssembly, so packaging has no extra asset to ship.
  const quickjs = await newQuickJSWASMModuleFromVariant(
    // CommonJS interop types this as the module; the core unwraps its `default` at runtime.
    variantModule.default as unknown as Parameters<typeof newQuickJSWASMModuleFromVariant>[0],
  );
  const runtime = quickjs.newRuntime();
  runtime.setMemoryLimit(COMPARISON_MEMORY_LIMIT_BYTES);
  runtime.setMaxStackSize(COMPARISON_STACK_LIMIT_BYTES);
  const deadline = performance.now() + COMPARISON_TIME_LIMIT_MS;
  runtime.setInterruptHandler(() => performance.now() > deadline);
  const context = runtime.newContext();

  const prelude = context.evalCode(PRELUDE, "prelude.js");
  if (prelude.error) return { ok: false, error: describeError(context, prelude.error) };
  const loaded = context.evalCode(request.source, "comparison.js");
  if (loaded.error) return { ok: false, error: describeError(context, loaded.error) };
  const compare = context.getProp(context.global, "compare");
  if (context.typeof(compare) !== "function") {
    return { ok: false, error: "The source must define a function named compare." };
  }
  if (request.operation === "compile") return { ok: true, result: null };

  const input = context.newString(JSON.stringify(request.input));
  const called = context.callFunction(prelude.value, context.undefined, compare, input);
  if (called.error) return { ok: false, error: describeError(context, called.error) };
  if (context.typeof(called.value) !== "string") {
    return { ok: false, error: "compare did not return a JSON result." };
  }
  return { ok: true, result: context.getString(called.value) };
}

/** Reads one request from stdin and answers on stdout. The process exists for this one run. */
export async function runCommandWatchComparisonWorker(): Promise<void> {
  const chunks: Array<Buffer> = [];
  for await (const chunk of process.stdin) chunks.push(chunk as Buffer);
  const request = JSON.parse(Buffer.concat(chunks).toString("utf8")) as ComparisonWorkerRequest;
  const response = await runComparison(request);
  // Exit without disposing: the process and everything the comparison allocated end here.
  process.stdout.write(JSON.stringify(response), () => process.exit(0));
}
