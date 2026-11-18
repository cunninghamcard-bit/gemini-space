import { randomUUID } from "node:crypto";
import vm from "node:vm";
import type { AgyBridge, BridgeToolCallResult, ExposedMcpTool } from "./agy-bridge.js";
import { cleanGeminiToolSchema } from "./schema-clean.js";

const MAX_RUNNING_EXECUTIONS = 6;
const MAX_SCRIPT_MS = 5 * 60_000;
const MAX_CALLS_PER_SCRIPT = 64;
const COMPLETED_TTL_MS = 10 * 60_000;

export type CodeExecutionStatus = "running" | "completed" | "failed" | "cancelled";

export interface CodeToolCallTrace {
  tool: string;
  durationMs: number;
  isError: boolean;
}

export interface CodeExecutionSnapshot {
  execution_id: string;
  status: CodeExecutionStatus;
  running: boolean;
  result?: unknown;
  logs: string[];
  calls: CodeToolCallTrace[];
  error?: string;
}

interface ActiveExecution {
  id: string;
  mcpSessionId: string;
  status: CodeExecutionStatus;
  controller: AbortController;
  logs: string[];
  calls: CodeToolCallTrace[];
  result?: unknown;
  error?: string;
  done: Promise<void>;
  waiters: Set<() => void>;
  cleanupTimer?: NodeJS.Timeout;
}

export const CODE_MODE_TOOL_NAMES = [
  "execute",
  "poll_execution",
  "cancel_execution",
] as const;

export function codeModeToolDefinitions(holdMs: number): ExposedMcpTool[] {
  const holdSec = Math.round(holdMs / 1000);
  return [
    {
      name: "execute",
      description: [
        "Execute an async JavaScript snippet on the local Mac to batch multiple agy tool calls in one round trip.",
        "Available globals: `tools.read_file(args)`, `tools.write_file(args)`, `tools.edit_file(args)`, `tools.run_command(args)`, `tools.manage_task(args)` (each returns `{ text, isError, content }`), `console.log(...)`, `sleep(ms)`, `fetch`, `Buffer`, `URL`.",
        `Use \`return <value>\` to return JSON-serializable data. Holds up to ${holdSec}s; if still running, returns \`execution_id\` for \`poll_execution\`.`,
      ].join(" "),
      inputSchema: cleanGeminiToolSchema({
        type: "object",
        properties: {
          code: {
            type: "string",
            description: "Async JavaScript body to run (top-level await and return are supported).",
          },
          yield_time_ms: {
            type: "integer",
            minimum: 500,
            maximum: holdMs,
            description: `Optional max milliseconds to wait before returning execution_id (up to ${holdMs}).`,
          },
        },
        required: ["code"],
      }),
    },
    {
      name: "poll_execution",
      description: `Wait up to ${holdSec}s for a running \`execute\` script to finish without restarting it.`,
      inputSchema: cleanGeminiToolSchema({
        type: "object",
        properties: {
          execution_id: {
            type: "string",
            description: "The execution_id returned by execute.",
          },
          yield_time_ms: {
            type: "integer",
            minimum: 500,
            maximum: holdMs,
            description: `Optional max milliseconds to wait (up to ${holdMs}).`,
          },
        },
        required: ["execution_id"],
      }),
    },
    {
      name: "cancel_execution",
      description: "Abort a running `execute` script.",
      inputSchema: cleanGeminiToolSchema({
        type: "object",
        properties: {
          execution_id: {
            type: "string",
            description: "The execution_id returned by execute.",
          },
        },
        required: ["execution_id"],
      }),
    },
  ];
}

export class CodeModeManager {
  private readonly executions = new Map<string, ActiveExecution>();
  private closed = false;

  constructor(
    private readonly bridge: AgyBridge,
    private readonly defaultHoldMs: number,
  ) {}

  isCodeModeTool(name: string): boolean {
    return (CODE_MODE_TOOL_NAMES as readonly string[]).includes(name);
  }

  async callTool(
    mcpSessionId: string | undefined,
    name: string,
    rawArgs: Record<string, unknown> | undefined,
  ): Promise<BridgeToolCallResult> {
    const args = rawArgs ?? {};
    const sessionKey = mcpSessionId?.trim() || "stateless";

    try {
      if (name === "execute") {
        const code = typeof args.code === "string" ? args.code : "";
        if (!code.trim()) {
          return errorResult("execute requires a non-empty `code` string.");
        }
        const yieldMs = clampYieldMs(args.yield_time_ms, this.defaultHoldMs);
        const snap = await this.start(sessionKey, code, yieldMs);
        return snapshotToResult(snap);
      }

      if (name === "poll_execution") {
        const id = typeof args.execution_id === "string" ? args.execution_id : "";
        const yieldMs = clampYieldMs(args.yield_time_ms, this.defaultHoldMs);
        const snap = await this.poll(id, yieldMs);
        return snapshotToResult(snap);
      }

      if (name === "cancel_execution") {
        const id = typeof args.execution_id === "string" ? args.execution_id : "";
        const snap = await this.cancel(id);
        return snapshotToResult(snap);
      }

      return errorResult(`Unknown Code Mode tool: ${name}`);
    } catch (error) {
      return errorResult(error instanceof Error ? error.message : String(error));
    }
  }

  async start(mcpSessionId: string, code: string, yieldMs: number): Promise<CodeExecutionSnapshot> {
    if (this.closed) {
      throw new Error("Server is shutting down; Code Mode cannot start new scripts.");
    }
    const runningCount = [...this.executions.values()].filter((e) => e.status === "running").length;
    if (runningCount >= MAX_RUNNING_EXECUTIONS) {
      throw new Error(
        `Already running ${MAX_RUNNING_EXECUTIONS} Code Mode scripts. Poll or cancel an existing execution first.`,
      );
    }

    const id = randomUUID();
    const execution: ActiveExecution = {
      id,
      mcpSessionId,
      status: "running",
      controller: new AbortController(),
      logs: [],
      calls: [],
      done: Promise.resolve(),
      waiters: new Set(),
    };
    this.executions.set(id, execution);
    execution.done = this.runScript(execution, code);

    await this.waitForCompletion(execution, yieldMs);
    return this.snapshot(execution);
  }

  async poll(id: string, yieldMs: number): Promise<CodeExecutionSnapshot> {
    const execution = this.executions.get(id);
    if (!execution) {
      throw new Error(`Unknown or expired execution_id: ${id}`);
    }
    if (execution.status === "running") {
      await this.waitForCompletion(execution, yieldMs);
    }
    return this.snapshot(execution);
  }

  async cancel(id: string): Promise<CodeExecutionSnapshot> {
    const execution = this.executions.get(id);
    if (!execution) {
      throw new Error(`Unknown or expired execution_id: ${id}`);
    }
    if (execution.status === "running") {
      execution.controller.abort(new Error("Execution cancelled by caller."));
      await this.waitForCompletion(execution, 2_000);
    }
    return this.snapshot(execution);
  }

  async close(): Promise<void> {
    this.closed = true;
    for (const execution of this.executions.values()) {
      if (execution.cleanupTimer) clearTimeout(execution.cleanupTimer);
      if (execution.status === "running") {
        execution.controller.abort(new Error("Server shutting down."));
      }
      for (const w of [...execution.waiters]) w();
    }
    this.executions.clear();
  }

  private waitForCompletion(execution: ActiveExecution, timeoutMs: number): Promise<void> {
    if (execution.status !== "running") return Promise.resolve();
    return new Promise((resolveWait) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        execution.waiters.delete(finish);
        resolveWait();
      };
      const timer = setTimeout(finish, timeoutMs);
      execution.waiters.add(finish);
    });
  }

  private snapshot(execution: ActiveExecution): CodeExecutionSnapshot {
    return {
      execution_id: execution.id,
      status: execution.status,
      running: execution.status === "running",
      ...(execution.result !== undefined ? { result: execution.result } : {}),
      logs: [...execution.logs],
      calls: [...execution.calls],
      ...(execution.error !== undefined ? { error: execution.error } : {}),
    };
  }

  private async runScript(execution: ActiveExecution, code: string): Promise<void> {
    const { signal } = execution.controller;
    const scriptTimeout = setTimeout(() => {
      execution.controller.abort(new Error(`Code Mode script exceeded ${MAX_SCRIPT_MS / 1000}s limit.`));
    }, MAX_SCRIPT_MS);

    let callCount = 0;
    const makeToolWrapper = (toolName: string) => async (toolArgs: Record<string, unknown> = {}) => {
      if (signal.aborted) {
        throw new Error(String(signal.reason instanceof Error ? signal.reason.message : "Execution cancelled."));
      }
      callCount += 1;
      if (callCount > MAX_CALLS_PER_SCRIPT) {
        throw new Error(`Code Mode script exceeded limit of ${MAX_CALLS_PER_SCRIPT} tool calls.`);
      }
      const t0 = Date.now();
      const res = await this.bridge.callTool(execution.mcpSessionId, toolName, toolArgs);
      const durationMs = Date.now() - t0;
      const text = res.content
        .map((c) => (c.type === "text" ? c.text : ""))
        .filter(Boolean)
        .join("\n");
      execution.calls.push({
        tool: toolName,
        durationMs,
        isError: Boolean(res.isError),
      });
      if (res.isError) {
        throw new Error(`${toolName} failed: ${text}`);
      }
      return { text, isError: false, content: res.content };
    };

    const pushLog = (prefix: string, items: unknown[]) => {
      if (execution.logs.length >= 200) return;
      const formatted = items
        .map((item) => (typeof item === "string" ? item : safeStringify(item)))
        .join(" ");
      execution.logs.push(prefix ? `[${prefix}] ${formatted}` : formatted);
    };

    const toolsObj = Object.freeze({
      read_file: makeToolWrapper("read_file"),
      write_file: makeToolWrapper("write_file"),
      edit_file: makeToolWrapper("edit_file"),
      run_command: makeToolWrapper("run_command"),
      manage_task: makeToolWrapper("manage_task"),
    });

    const sleep = (ms: number) =>
      new Promise<void>((resolveSleep, rejectSleep) => {
        if (signal.aborted) {
          rejectSleep(new Error("Execution cancelled."));
          return;
        }
        const t = setTimeout(() => {
          signal.removeEventListener("abort", onAbort);
          resolveSleep();
        }, Math.max(0, Number(ms) || 0));
        const onAbort = () => {
          clearTimeout(t);
          rejectSleep(new Error("Execution cancelled."));
        };
        signal.addEventListener("abort", onAbort, { once: true });
      });

    try {
      const context = vm.createContext({
        tools: toolsObj,
        ...toolsObj,
        console: Object.freeze({
          log: (...a: unknown[]) => pushLog("", a),
          info: (...a: unknown[]) => pushLog("info", a),
          warn: (...a: unknown[]) => pushLog("warn", a),
          error: (...a: unknown[]) => pushLog("error", a),
        }),
        sleep,
        fetch,
        Buffer,
        URL,
        URLSearchParams,
        TextEncoder,
        TextDecoder,
        AbortSignal: signal,
      });

      const wrappedSource = `(async () => {\n${code}\n})()`;
      const script = new vm.Script(wrappedSource, { filename: `codemode-${execution.id.slice(0, 8)}.js` });
      const returned = await (script.runInContext(context) as Promise<unknown>);
      if (signal.aborted) {
        execution.status = "cancelled";
        execution.error = signal.reason instanceof Error ? signal.reason.message : "Execution cancelled.";
      } else {
        execution.status = "completed";
        execution.result = normalizeSerializable(returned);
      }
    } catch (error) {
      if (signal.aborted) {
        execution.status = "cancelled";
        execution.error = signal.reason instanceof Error ? signal.reason.message : "Execution cancelled.";
      } else {
        execution.status = "failed";
        execution.error = error instanceof Error ? error.message : String(error);
      }
    } finally {
      clearTimeout(scriptTimeout);
      for (const w of [...execution.waiters]) w();
      execution.cleanupTimer = setTimeout(() => {
        this.executions.delete(execution.id);
      }, COMPLETED_TTL_MS);
      execution.cleanupTimer.unref();
    }
  }
}

function clampYieldMs(raw: unknown, maxHoldMs: number): number {
  if (typeof raw !== "number" || !Number.isFinite(raw)) return maxHoldMs;
  return Math.max(500, Math.min(Math.trunc(raw), maxHoldMs));
}

function normalizeSerializable(value: unknown): unknown {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(JSON.stringify(value)) as unknown;
  } catch {
    return String(value);
  }
}

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function snapshotToResult(snap: CodeExecutionSnapshot): BridgeToolCallResult {
  const isError = snap.status === "failed";
  return {
    content: [{ type: "text", text: JSON.stringify(snap, null, 2) }],
    ...(isError ? { isError: true } : {}),
  };
}

function errorResult(message: string): BridgeToolCallResult {
  return {
    content: [{ type: "text", text: message }],
    isError: true,
  };
}
