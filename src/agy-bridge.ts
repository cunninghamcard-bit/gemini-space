import { spawn, type ChildProcessByStdio } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, statSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { resolve } from "node:path";
import type { Readable, Writable } from "node:stream";
import type { AgyBridgeConfig } from "./config.js";
import { commandPreview, logEvent, type LoggingConfig } from "./logger.js";
import { cleanGeminiToolSchema } from "./schema-clean.js";

export interface AgyFunctionDeclaration {
  name: string;
  description?: string;
  parameters?: unknown;
  parametersJsonSchema?: unknown;
}

export interface ExposedMcpTool {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
}

export type McpContentBlock =
  | { type: "text"; text: string }
  | { type: "image"; data: string; mimeType: string }
  | { type: "audio"; data: string; mimeType: string }
  | { type: "resource"; resource: { uri: string; mimeType: string; blob: string } };

export interface BridgeToolCallResult {
  [key: string]: unknown;
  content: McpContentBlock[];
  isError?: boolean;
}

interface InlineMedia {
  mimeType: string;
  data: string;
}

interface ParsedToolResult {
  callId?: string;
  name?: string;
  text: string;
  images: InlineMedia[];
}

interface PendingGatewayCall {
  token: string;
  agySessionId: string;
  name: string;
  args: Record<string, unknown>;
  issued: boolean;
  callId?: string;
  result?: ParsedToolResult;
}

interface SessionNotice {
  ts: string;
  sender: string;
  text: string;
  delivered: boolean;
}

class McpSessionEntry {
  agySessionId: string | undefined;
  turnCount = 0;
  lastUsedAt = Date.now();
  private chain: Promise<void> = Promise.resolve();
  readonly notices: SessionNotice[] = [];
  readonly seenNotices = new Set<string>();
  readonly runningTasks = new Set<string>();
  readonly completedTaskOutputs = new Map<string, string>();
  readonly noticeWaiters = new Set<() => void>();

  constructor(readonly mcpSessionId: string) {}

  serial<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.chain.then(fn, fn);
    this.chain = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  waitForTask(taskId: string, timeoutMs: number): Promise<void> {
    return new Promise((resolveWait) => {
      let finished = false;
      const done = () => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        this.noticeWaiters.delete(check);
        resolveWait();
      };
      const check = () => {
        if (!this.runningTasks.has(taskId)) done();
      };
      const timer = setTimeout(done, timeoutMs);
      this.noticeWaiters.add(check);
      check();
    });
  }

  wakeAllWaiters(): void {
    for (const waiter of [...this.noticeWaiters]) {
      waiter();
    }
  }

  takeFreshNotices(excludeTaskId?: string): string {
    const fresh = this.notices.filter((n) => !n.delivered);
    for (const n of fresh) {
      n.delivered = true;
      this.completedTaskOutputs.delete(n.sender);
    }
    const extra = excludeTaskId ? fresh.filter((n) => n.sender !== excludeTaskId) : fresh;
    if (extra.length === 0) return "";
    return "\n\n" + extra.map((n) => `[Notice ${n.ts}] ${n.text}`).join("\n\n");
  }
}

interface AgyRuntime {
  proc: ChildProcessByStdio<Writable, Readable, Readable>;
  acpCall(method: string, params: Record<string, unknown>): Promise<Record<string, unknown>>;
  acpNotify(method: string, params: Record<string, unknown>): void;
  close(): Promise<void>;
}

export function buildServerInstructions(holdMs: number): string {
  const holdSeconds = Math.round(holdMs / 1000);
  return [
    "These tools run directly on the owner's Mac through Antigravity (agy) with full local filesystem and zsh permissions.",
    "Always pass absolute file paths to read_file, write_file, and edit_file; no workspace-open step is needed.",
    "Before making changes in an unfamiliar repository, check whether AGENTS.md or CLAUDE.md exists at its root with read_file and follow its rules.",
    `Non-daemon run_command calls automatically wait on the server for up to ${holdSeconds} seconds so most commands finish in one call.`,
    `If run_command exceeds ${holdSeconds} seconds and returns a background task id, call manage_task with Action "status" and that TaskId: the server holds each status check up to ${holdSeconds} seconds until the task finishes. Repeat until completion when your task depends on the output.`,
    "Completion notices for background tasks in your session are also appended to subsequent tool results.",
  ].join(" ");
}

export class AgyBridge {
  private gatewayServer: http.Server | undefined;
  private gatewayPort = 0;
  private runtime: AgyRuntime | undefined;
  private runtimeBinMtimeMs = 0;
  private runtimePromise: Promise<AgyRuntime> | undefined;
  private decls: AgyFunctionDeclaration[] | undefined;
  private readonly declsWaiters: Array<() => void> = [];
  private readonly pendingCalls = new Map<string, PendingGatewayCall>();
  private readonly mcpSessions = new Map<string, McpSessionEntry>();
  private readonly sessionsByAgyId = new Map<string, McpSessionEntry>();
  private readonly fileWriteLocks = new Map<string, Promise<void>>();
  private gcTimer: NodeJS.Timeout | undefined;
  private closing = false;

  constructor(
    private readonly config: AgyBridgeConfig,
    private readonly logging: LoggingConfig,
  ) {}

  async start(): Promise<void> {
    mkdirSync(this.config.agyHome, { recursive: true, mode: 0o700 });
    mkdirSync(this.config.workspace, { recursive: true });

    await this.startGateway();
    await this.ensureRuntimeAndDeclarations();

    this.gcTimer = setInterval(() => this.pruneIdleSessions(), 5 * 60_000);
    this.gcTimer.unref();
  }

  async close(): Promise<void> {
    this.closing = true;
    if (this.gcTimer) {
      clearInterval(this.gcTimer);
      this.gcTimer = undefined;
    }

    for (const entry of this.mcpSessions.values()) {
      entry.runningTasks.clear();
      entry.wakeAllWaiters();
    }

    const activeRuntime = this.runtime;
    this.runtime = undefined;
    this.runtimePromise = undefined;
    if (activeRuntime) {
      await activeRuntime.close();
    }

    if (this.gatewayServer) {
      const server = this.gatewayServer;
      this.gatewayServer = undefined;
      await new Promise<void>((resolveClose) => server.close(() => resolveClose()));
    }
  }

  async listTools(): Promise<ExposedMcpTool[]> {
    await this.ensureRuntimeAndDeclarations();
    const decls = this.decls ?? [];
    const allowed = new Set(this.config.exposedTools);
    return decls
      .filter((d) => allowed.has(d.name) && d.name !== "ask_question")
      .map((d) => ({
        name: d.name,
        description: d.description ?? "",
        inputSchema: cleanGeminiToolSchema(d.parameters ?? d.parametersJsonSchema ?? { type: "object" }),
      }));
  }

  isToolExposed(name: string): boolean {
    return name !== "ask_question" && this.config.exposedTools.includes(name);
  }

  dropMcpSession(mcpSessionId: string | undefined): void {
    const key = mcpSessionId?.trim();
    if (!key) return;
    const entry = this.mcpSessions.get(key);
    if (!entry || entry.runningTasks.size > 0) return;
    if (entry.agySessionId) {
      this.sessionsByAgyId.delete(entry.agySessionId);
    }
    this.mcpSessions.delete(key);
  }

  activeSessionCount(): number {
    return this.mcpSessions.size;
  }

  async callTool(
    mcpSessionId: string | undefined,
    name: string,
    rawArgs: Record<string, unknown> | undefined,
  ): Promise<BridgeToolCallResult> {
    if (!this.isToolExposed(name)) {
      return {
        content: [{ type: "text", text: `Unknown or disabled tool: ${name}` }],
        isError: true,
      };
    }

    const args = rawArgs ?? {};
    const targetEntry = this.resolveTargetSession(mcpSessionId, name, args);
    targetEntry.lastUsedAt = Date.now();

    const execute = async (): Promise<BridgeToolCallResult> => {
      // 1. If checking status on a task, wait if still running and prefer the full <SYSTEM_MESSAGE>
      //    completion payload (which preserves exit code and ~10KB output, unlike agy's status summary).
      if (
        name === "manage_task"
        && args.Action === "status"
        && typeof args.TaskId === "string"
      ) {
        if (targetEntry.runningTasks.has(args.TaskId)) {
          const holdStart = Date.now();
          await targetEntry.waitForTask(args.TaskId, this.config.holdMs);
          logEvent(this.logging, "debug", "agy_task_hold", {
            tool: "manage_task",
            taskId: args.TaskId,
            waitedMs: Date.now() - holdStart,
            finished: !targetEntry.runningTasks.has(args.TaskId),
          });
        }

        const finishedOutput = targetEntry.completedTaskOutputs.get(args.TaskId);
        if (finishedOutput !== undefined) {
          for (const n of targetEntry.notices) {
            if (n.sender === args.TaskId) n.delivered = true;
          }
          targetEntry.completedTaskOutputs.delete(args.TaskId);
          const extraNotices = targetEntry.takeFreshNotices(args.TaskId);
          return {
            content: [{ type: "text", text: finishedOutput + extraNotices }],
          };
        }
      }

      const callStart = Date.now();
      let { out, isError, images } = await this.executeSingleTurn(targetEntry, name, args);

      // 2. If run_command backgrounded a non-daemon command at agy's 10s cap, hold up to total holdMs.
      const bgMatch = name === "run_command" ? out.match(/background task with task id: (\S+)/) : null;
      const bgTaskId = bgMatch?.[1];
      let suppressNoticeForTask: string | undefined =
        name === "manage_task" && typeof args.TaskId === "string" ? args.TaskId : undefined;

      if (bgTaskId) {
        if (!targetEntry.completedTaskOutputs.has(bgTaskId)) {
          targetEntry.runningTasks.add(bgTaskId);
        }

        const waitMsArg = typeof args.WaitMsBeforeAsync === "number" ? args.WaitMsBeforeAsync : 5_000;
        const wantsSync = args.IsDaemon !== true && waitMsArg > 500;

        if (wantsSync) {
          const remainMs = Math.max(0, this.config.holdMs - (Date.now() - callStart));
          if (remainMs > 0 && targetEntry.runningTasks.has(bgTaskId)) {
            await targetEntry.waitForTask(bgTaskId, remainMs);
          }

          const finishedOutput = targetEntry.completedTaskOutputs.get(bgTaskId);
          if (finishedOutput !== undefined) {
            out = finishedOutput;
            suppressNoticeForTask = bgTaskId;
            for (const n of targetEntry.notices) {
              if (n.sender === bgTaskId) n.delivered = true;
            }
            targetEntry.completedTaskOutputs.delete(bgTaskId);
          } else if (targetEntry.runningTasks.has(bgTaskId)) {
            const holdSec = Math.round(this.config.holdMs / 1000);
            out += `\n\n[Server held ${holdSec}s; task is still running. Call manage_task with Action "status" and TaskId "${bgTaskId}" to wait up to another ${holdSec}s.]`;
          }
        }
      }

      const extraNotices = targetEntry.takeFreshNotices(suppressNoticeForTask);
      const content: McpContentBlock[] = [
        { type: "text", text: out + extraNotices },
        ...images.map((media): McpContentBlock => this.mediaToContentBlock(media, args)),
      ];

      logEvent(this.logging, "info", "tool_call", {
        tool: name,
        mcpSessionId: targetEntry.mcpSessionId.slice(0, 12),
        agySessionId: targetEntry.agySessionId?.slice(0, 12),
        durationMs: Date.now() - callStart,
        isError,
        ...(name === "run_command" && this.logging.shellCommands && typeof args.CommandLine === "string"
          ? { command: commandPreview(args.CommandLine) }
          : {}),
      });

      return isError ? { content, isError: true } : { content };
    };

    if ((name === "write_file" || name === "edit_file") && typeof args.Path === "string") {
      return this.withFileWriteLock(args.Path, execute);
    }

    return execute();
  }

  private mediaToContentBlock(media: InlineMedia, args: Record<string, unknown>): McpContentBlock {
    if (media.mimeType.startsWith("image/")) {
      return { type: "image", data: media.data, mimeType: media.mimeType };
    }
    if (media.mimeType.startsWith("audio/")) {
      return { type: "audio", data: media.data, mimeType: media.mimeType };
    }
    const filePath = typeof args.Path === "string" ? args.Path : "/attachment";
    return {
      type: "resource",
      resource: {
        uri: `file://${filePath}`,
        mimeType: media.mimeType,
        blob: media.data,
      },
    };
  }

  private resolveTargetSession(
    mcpSessionId: string | undefined,
    toolName: string,
    args: Record<string, unknown>,
  ): McpSessionEntry {
    // If manage_task targets a specific "<agySessionId>/task-N", route to the owning agy session.
    if (toolName === "manage_task" && typeof args.TaskId === "string" && args.TaskId.includes("/")) {
      const prefix = args.TaskId.split("/")[0]!;
      const owner = this.sessionsByAgyId.get(prefix);
      if (owner) return owner;
    }

    const key = mcpSessionId?.trim() || "stateless";
    let entry = this.mcpSessions.get(key);
    if (!entry) {
      entry = new McpSessionEntry(key);
      this.mcpSessions.set(key, entry);
    }
    return entry;
  }

  private async withFileWriteLock<T>(rawPath: string, fn: () => Promise<T>): Promise<T> {
    const normalized = resolve(this.config.workspace, rawPath);
    const prev = this.fileWriteLocks.get(normalized) ?? Promise.resolve();
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    const chained = prev.then(() => gate);
    this.fileWriteLocks.set(normalized, chained);

    await prev;
    try {
      return await fn();
    } finally {
      release();
      if (this.fileWriteLocks.get(normalized) === chained) {
        this.fileWriteLocks.delete(normalized);
      }
    }
  }

  private async executeSingleTurn(
    entry: McpSessionEntry,
    name: string,
    args: Record<string, unknown>,
  ): Promise<{ out: string; isError: boolean; images: InlineMedia[] }> {
    return entry.serial(async () => {
      const runtime = await this.ensureRuntimeAndDeclarations();

      // Rotate session when idle and over turn threshold to prevent unbounded prompt history growth.
      if (
        entry.agySessionId
        && entry.turnCount >= this.config.maxTurnsPerSession
        && entry.runningTasks.size === 0
      ) {
        this.sessionsByAgyId.delete(entry.agySessionId);
        entry.agySessionId = undefined;
        entry.turnCount = 0;
      }

      if (!entry.agySessionId) {
        const created = await runtime.acpCall("session/new", {
          cwd: this.config.workspace,
          mcpServers: [],
        });
        const sid = String(created.sessionId ?? "");
        if (!sid) throw new Error("agy session/new did not return a sessionId");
        entry.agySessionId = sid;
        entry.turnCount = 0;
        this.sessionsByAgyId.set(sid, entry);
      }

      const agySessionId = entry.agySessionId;
      const token = randomUUID();
      const pending: PendingGatewayCall = {
        token,
        agySessionId,
        name,
        args,
        issued: false,
      };
      this.pendingCalls.set(token, pending);

      let timeoutHandle: NodeJS.Timeout | undefined;
      try {
        await Promise.race([
          runtime.acpCall("session/prompt", {
            sessionId: agySessionId,
            prompt: [{ type: "text", text: `synrex-call:${token}` }],
          }),
          new Promise<never>((_, reject) => {
            timeoutHandle = setTimeout(() => {
              runtime.acpNotify("session/cancel", { sessionId: agySessionId });
              reject(new Error(`agy did not finish ${name} within ${this.config.callTimeoutMs} ms`));
            }, this.config.callTimeoutMs);
          }),
        ]);
      } finally {
        if (timeoutHandle) clearTimeout(timeoutHandle);
        this.pendingCalls.delete(token);
      }

      entry.turnCount += 1;

      if (!pending.result) {
        throw new Error(`agy did not execute tool ${name}`);
      }

      let out = pending.result.text;
      let isError = false;
      try {
        const parsed = JSON.parse(out) as { output?: unknown; error?: unknown };
        if (typeof parsed.output === "string") {
          out = parsed.output;
        } else if (typeof parsed.error === "string") {
          out = parsed.error;
          isError = true;
        }
      } catch {
        // Raw text result
      }

      return {
        out,
        isError,
        images: pending.result.images,
      };
    });
  }

  private pruneIdleSessions(): void {
    const now = Date.now();
    for (const [key, entry] of this.mcpSessions) {
      if (
        entry.runningTasks.size === 0
        && now - entry.lastUsedAt > this.config.sessionIdleTtlMs
      ) {
        if (entry.agySessionId) {
          this.sessionsByAgyId.delete(entry.agySessionId);
        }
        this.mcpSessions.delete(key);
      }
    }
  }

  private hasActiveWork(): boolean {
    if (this.pendingCalls.size > 0) return true;
    for (const entry of this.mcpSessions.values()) {
      if (entry.runningTasks.size > 0) return true;
    }
    return false;
  }

  private currentBinMtimeMs(): number {
    try {
      return statSync(this.config.bin).mtimeMs;
    } catch {
      return 0;
    }
  }

  private async ensureRuntimeAndDeclarations(): Promise<AgyRuntime> {
    if (this.closing) {
      throw new Error("AGY bridge is shutting down");
    }
    // Hot-reload agy acp if ~/.local/bin/agy was auto-updated on disk while idle.
    if (this.runtime && !this.hasActiveWork()) {
      const mtime = this.currentBinMtimeMs();
      if (mtime > 0 && this.runtimeBinMtimeMs > 0 && mtime !== this.runtimeBinMtimeMs) {
        logEvent(this.logging, "info", "agy_binary_updated_reloading", {
          bin: this.config.bin,
        });
        const old = this.runtime;
        this.runtime = undefined;
        this.decls = undefined;
        this.sessionsByAgyId.clear();
        for (const entry of this.mcpSessions.values()) {
          entry.agySessionId = undefined;
          entry.turnCount = 0;
        }
        await old.close();
      }
    }
    if (this.runtime && this.decls) {
      return this.runtime;
    }
    if (!this.runtimePromise) {
      this.runtimePromise = this.spawnAndWarmup().finally(() => {
        this.runtimePromise = undefined;
      });
    }
    return this.runtimePromise;
  }

  private async spawnAndWarmup(): Promise<AgyRuntime> {
    const binMtime = this.currentBinMtimeMs();
    const proc = spawn(this.config.bin, ["acp", "--disable-telemetry"], {
      cwd: this.config.workspace,
      env: {
        ...process.env,
        HOME: this.config.agyHome,
        AGY_LLM_GATEWAY_URL: `http://127.0.0.1:${this.gatewayPort}`,
        AGY_LLM_GATEWAY_API_KEY: "devspace-local",
        AGY_LLM_GATEWAY_WIRE_PROTOCOL: this.config.wireProtocol,
      },
      stdio: ["pipe", "pipe", "pipe"],
    });

    let acpBuffer = "";
    let nextRpcId = 0;
    const acpPending = new Map<
      number,
      { resolve: (val: Record<string, unknown>) => void; reject: (err: Error) => void }
    >();

    const acpWrite = (payload: Record<string, unknown>) => {
      if (!proc.stdin.destroyed && proc.stdin.writable) {
        proc.stdin.write(`${JSON.stringify(payload)}\n`);
      }
    };

    const acpCall = (method: string, params: Record<string, unknown>) =>
      new Promise<Record<string, unknown>>((resolveRpc, rejectRpc) => {
        const id = ++nextRpcId;
        acpPending.set(id, { resolve: resolveRpc, reject: rejectRpc });
        acpWrite({ jsonrpc: "2.0", id, method, params });
      });

    const acpNotify = (method: string, params: Record<string, unknown>) => {
      acpWrite({ jsonrpc: "2.0", method, params });
    };

    proc.stderr.on("data", (chunk: Buffer) => {
      const text = chunk.toString().trim();
      if (text) {
        logEvent(this.logging, "debug", "agy_stderr", { message: text.slice(0, 500) });
      }
    });

    proc.stdout.on("data", (chunk: Buffer) => {
      acpBuffer += chunk.toString();
      let newlineIdx: number;
      while ((newlineIdx = acpBuffer.indexOf("\n")) >= 0) {
        const line = acpBuffer.slice(0, newlineIdx).trim();
        acpBuffer = acpBuffer.slice(newlineIdx + 1);
        if (!line) continue;

        let msg: Record<string, unknown>;
        try {
          msg = JSON.parse(line) as Record<string, unknown>;
        } catch {
          continue;
        }

        if (typeof msg.id === "number" && typeof msg.method !== "string") {
          const pendingRpc = acpPending.get(msg.id);
          acpPending.delete(msg.id);
          if (pendingRpc) {
            if (msg.error) {
              pendingRpc.reject(new Error(JSON.stringify(msg.error)));
            } else {
              pendingRpc.resolve((msg.result as Record<string, unknown>) ?? {});
            }
          }
        } else if (msg.method === "session/request_permission" && msg.id !== undefined) {
          const params = (msg.params ?? {}) as {
            options?: Array<{ kind?: string; optionId: string }>;
            toolCall?: { name?: string };
          };
          const options = params.options ?? [];
          const chosen =
            options.find((o) => o.kind === "allow_always")
            ?? options.find((o) => o.kind?.startsWith("allow"))
            ?? options[0];
          if (chosen) {
            logEvent(this.logging, "debug", "agy_permission_approved", {
              tool: params.toolCall?.name ?? null,
              optionId: chosen.optionId,
            });
            acpWrite({
              jsonrpc: "2.0",
              id: msg.id,
              result: { outcome: { outcome: "selected", optionId: chosen.optionId } },
            });
          }
        } else if (typeof msg.method === "string" && msg.id !== undefined) {
          acpWrite({
            jsonrpc: "2.0",
            id: msg.id,
            error: { code: -32601, message: "Method not supported by DevSpace AGY bridge" },
          });
        }
      }
    });

    const handleExit = (code: number | null, signal: NodeJS.Signals | null) => {
      if (this.runtime?.proc === proc) {
        this.runtime = undefined;
      }
      const err = new Error(`agy acp exited (code=${code}, signal=${signal})`);
      for (const [, waiter] of acpPending) {
        waiter.reject(err);
      }
      acpPending.clear();

      // Clear stale agySessionId bindings so sessions transparently re-open on next call.
      this.sessionsByAgyId.clear();
      for (const entry of this.mcpSessions.values()) {
        entry.agySessionId = undefined;
        entry.turnCount = 0;
        entry.runningTasks.clear();
        entry.wakeAllWaiters();
      }

      if (!this.closing) {
        logEvent(this.logging, "warn", "agy_process_exited", {
          code,
          signal,
        });
      }
    };

    proc.once("exit", handleExit);

    const runtime: AgyRuntime = {
      proc,
      acpCall,
      acpNotify,
      close: async () => {
        proc.off("exit", handleExit);
        await new Promise<void>((resolveStop) => {
          const killTimer = setTimeout(() => {
            proc.kill("SIGKILL");
          }, 1_500);
          proc.once("exit", () => {
            clearTimeout(killTimer);
            resolveStop();
          });
          try {
            proc.stdin.end();
            proc.kill("SIGTERM");
          } catch {
            clearTimeout(killTimer);
            resolveStop();
          }
        });
      },
    };

    await acpCall("initialize", {
      protocolVersion: 1,
      clientCapabilities: {
        fs: { readTextFile: false, writeTextFile: false },
        terminal: false,
      },
      clientInfo: { name: "devspace-agy-bridge", version: "1.0.0" },
    });

    if (!this.decls) {
      const warmup = await acpCall("session/new", {
        cwd: this.config.workspace,
        mcpServers: [],
      });
      const warmupSid = String(warmup.sessionId ?? "");
      await acpCall("session/prompt", {
        sessionId: warmupSid,
        prompt: [{ type: "text", text: "synrex-warmup" }],
      });
      if (!this.decls) {
        await new Promise<void>((r) => this.declsWaiters.push(r));
      }
    }

    this.runtime = runtime;
    this.runtimeBinMtimeMs = binMtime;
    logEvent(this.logging, "info", "agy_bridge_ready", {
      pid: proc.pid ?? null,
      gatewayPort: this.gatewayPort,
      declaredTools: (this.decls ?? []).length,
      exposedTools: this.config.exposedTools.join(","),
    });

    return runtime;
  }

  private async startGateway(): Promise<void> {
    if (this.gatewayServer) return;

    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        try {
          this.handleGatewayRequest(req, res, Buffer.concat(chunks).toString("utf8"));
        } catch (error) {
          logEvent(this.logging, "error", "agy_gateway_error", {
            error: error instanceof Error ? error.message : String(error),
          });
          if (!res.headersSent) {
            res.writeHead(500);
          }
          res.end();
        }
      });
    });

    await new Promise<void>((resolveListen, rejectListen) => {
      server.once("error", rejectListen);
      server.listen(0, "127.0.0.1", () => {
        server.off("error", rejectListen);
        const addr = server.address() as AddressInfo;
        this.gatewayPort = addr.port;
        this.gatewayServer = server;
        resolveListen();
      });
    });
  }

  private handleGatewayRequest(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    rawBody: string,
  ): void {
    const url = req.url ?? "";
    const wire = this.config.wireProtocol;
    const defaultModel = this.config.modelId;

    if (req.method === "GET" && url.includes("/models")) {
      res.writeHead(200, { "content-type": "application/json" });
      if (wire === "openai") {
        res.end(
          JSON.stringify({
            object: "list",
            data: [{ id: defaultModel, object: "model", owned_by: "devspace" }],
          }),
        );
        return;
      }
      res.end(
        JSON.stringify({
          models: [
            {
              name: `models/${defaultModel}`,
              baseModelId: defaultModel,
              version: "1",
              displayName: defaultModel,
              inputTokenLimit: 1_048_576,
              outputTokenLimit: 65_536,
              supportedGenerationMethods: [
                "generateContent",
                "streamGenerateContent",
                "countTokens",
              ],
            },
          ],
        }),
      );
      return;
    }

    const isGenEndpoint = wire === "openai"
      ? url.includes("chat/completions")
      : /:(stream)?[gG]enerateContent/.test(url);

    if (req.method !== "POST" || !isGenEndpoint) {
      res.writeHead(404);
      res.end();
      return;
    }

    const body = JSON.parse(rawBody) as Record<string, unknown>;
    const nonStream = wire !== "openai" && /:generateContent/.test(url);
    const model = typeof body.model === "string" ? body.model : defaultModel;
    const parsed = this.parseGatewayBody(body);
    const labels = body.labels as { cascade_id?: string } | undefined;
    const cascadeId = labels?.cascade_id;

    const rawTools = Array.isArray(body.tools) ? (body.tools as Array<Record<string, unknown>>) : [];
    if (rawTools.some((t) => Boolean(t.googleSearch || t.google_search))) {
      this.replyText(res, nonStream, model, "Web search is disabled on this local AGY bridge.");
      return;
    }

    if (parsed.tools.length === 0) {
      this.replyText(res, nonStream, model, "DevSpace AGY Bridge");
      return;
    }

    if (!this.decls) {
      this.decls = parsed.tools;
      for (const waiter of this.declsWaiters.splice(0)) {
        waiter();
      }
    }

    if (cascadeId) {
      this.scanSessionNotices(cascadeId, parsed.turns);
      // Bug fix: when a background task finishes during a foreground tool call on the same session,
      // agy appends <SYSTEM_MESSAGE> inside functionResponse.response.output rather than a user text part.
      if (parsed.toolResult?.text) {
        try {
          const respObj = JSON.parse(parsed.toolResult.text) as { output?: unknown };
          if (typeof respObj.output === "string" && respObj.output.includes("<SYSTEM_MESSAGE>")) {
            this.scanSessionNotices(cascadeId, [{ role: "user", text: respObj.output }]);
            // Strip the embedded <SYSTEM_MESSAGE> block from the foreground tool's raw output so
            // notices are delivered cleanly once via SessionNotice without duplication.
            const stripped = respObj.output.replace(
              /\n*The following is a <SYSTEM_MESSAGE>[\s\S]*<\/SYSTEM_MESSAGE>\s*$/,
              "",
            );
            parsed.toolResult.text = JSON.stringify({ ...respObj, output: stripped });
          }
        } catch {
          if (parsed.toolResult.text.includes("<SYSTEM_MESSAGE>")) {
            this.scanSessionNotices(cascadeId, [{ role: "user", text: parsed.toolResult.text }]);
          }
        }
      }
    }

    if (parsed.toolResult) {
      const tr = parsed.toolResult;
      const match = [...this.pendingCalls.values()].find(
        (p) =>
          p.issued
          && p.result === undefined
          && (!cascadeId || p.agySessionId === cascadeId)
          && (p.callId === tr.callId || (!tr.callId && p.name === tr.name)),
      );
      if (match) {
        match.result = tr;
      }
      this.replyText(res, nonStream, model, "OK");
      return;
    }

    for (const [token, call] of this.pendingCalls) {
      if (
        !call.issued
        && (!cascadeId || call.agySessionId === cascadeId)
        && rawBody.includes(`synrex-call:${token}`)
      ) {
        call.issued = true;
        call.callId = `call_${token.slice(0, 8)}`;
        this.replyToolCall(res, nonStream, model, call.callId, call.name, call.args);
        return;
      }
    }

    this.replyText(res, nonStream, model, "OK");
  }

  private scanSessionNotices(
    cascadeId: string,
    turns: Array<{ role: string; text: string }>,
  ): void {
    const entry = this.sessionsByAgyId.get(cascadeId);
    if (!entry) return;

    const msgHeaderRe =
      /\[Message\] timestamp=(\d{4}-\d{2}-\d{2}T\S+) sender=(\S+) priority=\S+ content=/g;

    for (const turn of turns) {
      if (turn.role !== "user" || !turn.text.includes("<SYSTEM_MESSAGE>")) continue;
      const openIdx = turn.text.indexOf("<SYSTEM_MESSAGE>");
      const closeIdx = turn.text.lastIndexOf("</SYSTEM_MESSAGE>");
      if (openIdx < 0 || closeIdx <= openIdx) continue;

      const block = turn.text.slice(openIdx + "<SYSTEM_MESSAGE>".length, closeIdx);
      const headers = [...block.matchAll(msgHeaderRe)];
      for (let i = 0; i < headers.length; i += 1) {
        const hdr = headers[i]!;
        const ts = hdr[1]!;
        const sender = hdr[2]!;
        const contentStart = (hdr.index ?? 0) + hdr[0].length;
        const contentEnd = i + 1 < headers.length ? (headers[i + 1]!.index ?? block.length) : block.length;
        const text = block.slice(contentStart, contentEnd).trim();
        const dedupeKey = `${ts} ${sender}`;
        if (entry.seenNotices.has(dedupeKey)) continue;

        entry.seenNotices.add(dedupeKey);
        entry.notices.push({ ts, sender, text, delivered: false });
        entry.completedTaskOutputs.set(sender, text);
        entry.runningTasks.delete(sender);
        entry.wakeAllWaiters();
      }
    }
  }

  private parseGatewayBody(body: Record<string, unknown>): {
    tools: AgyFunctionDeclaration[];
    turns: Array<{ role: string; text: string }>;
    toolResult?: ParsedToolResult;
  } {
    if (this.config.wireProtocol === "openai") {
      const msgs = (body.messages as Array<Record<string, unknown>> | undefined) ?? [];
      const last = msgs.at(-1);
      const rawTools = (body.tools as Array<{ function?: AgyFunctionDeclaration }> | undefined) ?? [];
      return {
        tools: rawTools.flatMap((t) => (t.function ? [t.function] : [])),
        turns: msgs.map((m) => ({
          role: String(m.role ?? ""),
          text: extractOpenAiText(m.content),
        })),
        toolResult:
          last?.role === "tool"
            ? {
                callId: typeof last.tool_call_id === "string" ? last.tool_call_id : undefined,
                text: extractOpenAiText(last.content),
                images: [],
              }
            : undefined,
      };
    }

    const contents = (body.contents as Array<Record<string, unknown>> | undefined) ?? [];
    const last = contents.at(-1);
    const lastParts = (last?.parts as Array<Record<string, unknown>> | undefined) ?? [];
    const fnRespPart = last?.role === "user"
      ? lastParts.find((p) => Boolean(p.functionResponse))
      : undefined;

    let toolResult: ParsedToolResult | undefined;
    if (fnRespPart) {
      const fr = fnRespPart.functionResponse as {
        id?: string;
        name?: string;
        response?: unknown;
        parts?: Array<Record<string, unknown>>;
      };
      const images: InlineMedia[] = [];
      for (const part of [...lastParts, ...(fr.parts ?? [])]) {
        const inline = (part.inlineData ?? part.inline_data) as
          | { data?: string; mimeType?: string; mime_type?: string }
          | undefined;
        if (inline?.data) {
          images.push({
            mimeType: inline.mimeType ?? inline.mime_type ?? "application/octet-stream",
            data: inline.data,
          });
        }
      }
      toolResult = {
        callId: fr.id,
        name: fr.name,
        text: JSON.stringify(fr.response ?? {}),
        images,
      };
    }

    const rawToolGroups =
      (body.tools as Array<{ functionDeclarations?: AgyFunctionDeclaration[] }> | undefined) ?? [];
    const tools = rawToolGroups
      .flatMap((g) => g.functionDeclarations ?? [])
      .map((d) => ({
        ...d,
        parameters: d.parameters ?? d.parametersJsonSchema,
      }));

    const turns = contents.map((c) => {
      const parts = (c.parts as Array<{ text?: string }> | undefined) ?? [];
      return {
        role: String(c.role ?? ""),
        text: parts.map((p) => p.text ?? "").join(""),
      };
    });

    return { tools, turns, toolResult };
  }

  private sendGatewayChunks(
    res: http.ServerResponse,
    nonStream: boolean,
    chunks: unknown[],
  ): void {
    if (nonStream) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(chunks[0]));
      return;
    }
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
    });
    for (const chunk of chunks) {
      res.write(`data: ${JSON.stringify(chunk)}\n\n`);
    }
    res.end(this.config.wireProtocol === "openai" ? "data: [DONE]\n\n" : "");
  }

  private replyText(
    res: http.ServerResponse,
    nonStream: boolean,
    model: string,
    text: string,
  ): void {
    if (this.config.wireProtocol === "openai") {
      const base = {
        id: `chatcmpl-${Date.now()}`,
        object: "chat.completion.chunk",
        created: Math.floor(Date.now() / 1000),
        model,
      };
      this.sendGatewayChunks(res, nonStream, [
        { ...base, choices: [{ index: 0, delta: { role: "assistant", content: text }, finish_reason: null }] },
        {
          ...base,
          choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
          usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
        },
      ]);
      return;
    }

    this.sendGatewayChunks(res, nonStream, [
      {
        candidates: [
          {
            content: { role: "model", parts: [{ text }] },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 1, totalTokenCount: 2 },
        modelVersion: model,
      },
    ]);
  }

  private replyToolCall(
    res: http.ServerResponse,
    nonStream: boolean,
    model: string,
    callId: string,
    name: string,
    args: Record<string, unknown>,
  ): void {
    if (this.config.wireProtocol === "openai") {
      const base = {
        id: `chatcmpl-${Date.now()}`,
        object: "chat.completion.chunk",
        created: Math.floor(Date.now() / 1000),
        model,
      };
      const tc = {
        index: 0,
        id: callId,
        type: "function",
        function: { name, arguments: JSON.stringify(args) },
      };
      this.sendGatewayChunks(res, nonStream, [
        { ...base, choices: [{ index: 0, delta: { role: "assistant", content: null, tool_calls: [tc] }, finish_reason: null }] },
        {
          ...base,
          choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
          usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
        },
      ]);
      return;
    }

    this.sendGatewayChunks(res, nonStream, [
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ functionCall: { id: callId, name, args } }],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 1, totalTokenCount: 2 },
        modelVersion: model,
      },
    ]);
  }
}

function extractOpenAiText(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((part) => (part && typeof part === "object" && "text" in part ? String((part as { text?: unknown }).text ?? "") : ""))
      .join("");
  }
  return "";
}
