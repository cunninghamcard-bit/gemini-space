import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import type { AddressInfo } from "node:net";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { devspaceConfigSchema } from "./config-schema.js";
import type { ServerConfig } from "./config.js";
import { SingleUserOAuthProvider } from "./oauth-provider.js";
import { cleanGeminiToolSchema } from "./schema-clean.js";
import { createServer } from "./server.js";

const DEFAULT_AGY_BIN = join(homedir(), ".local", "bin", "agy");

// Minimal valid 1x1 red PNG
const TINY_PNG_BASE64 =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";

test("schema cleaner strips Gemini-incompatible keywords and lowercases types", () => {
  const dirty = {
    $schema: "https://json-schema.org/draft/2020-12/schema",
    type: "OBJECT",
    additionalProperties: false,
    properties: {
      Path: { type: "STRING", description: "Absolute path" },
      Count: { type: "INTEGER", exclusiveMinimum: 0, maximum: 9_999_999_999 },
      Mode: { const: "status" },
    },
    required: ["Path"],
  };

  const cleaned = cleanGeminiToolSchema(dirty);
  assert.equal("$schema" in cleaned, false);
  assert.equal("additionalProperties" in cleaned, false);
  assert.equal(cleaned.type, "object");

  const props = cleaned.properties as Record<string, Record<string, unknown>>;
  assert.equal(props.Path?.type, "string");
  assert.equal(props.Count?.type, "integer");
  assert.equal(props.Count?.minimum, 1);
  assert.equal("maximum" in (props.Count ?? {}), false);
  assert.deepEqual(props.Mode?.enum, ["status"]);
});

test("config schema accepts legacy Synrex keys and defaults refreshTokenGraceSeconds to 300", () => {
  const parsed = devspaceConfigSchema.parse({
    configVersion: 1,
    server: {
      host: "127.0.0.1",
      port: 7676,
      publicBaseUrl: "https://example.ts.net",
    },
    node: { alias: null, labels: {} },
    cluster: { role: "standalone" },
    workspaces: { allowedRoots: ["/Users/test"] },
    storage: { stateDir: "~/.local/share/devspace", metadataRetentionDays: 90 },
    tools: { mode: "codex" },
  });

  assert.equal(parsed.oauth.refreshTokenGraceSeconds, 300);
  assert.deepEqual(parsed.agy.exposedTools, [
    "read_file",
    "write_file",
    "edit_file",
    "run_command",
    "manage_task",
  ]);
});

test(
  "end-to-end AGY MCP bridge: tools, PNG read, multi-subagent concurrency, 10s+ single-call hold, and session isolation",
  { skip: !existsSync(DEFAULT_AGY_BIN) },
  async (t: TestContext) => {
    const root = await mkdtemp(join(tmpdir(), "devspace-agy-e2e-"));
    t.after(() => rm(root, { recursive: true, force: true }));

    const stateDir = join(root, "state");
    const agyHome = join(root, "agy-home");
    const workspace = join(root, "workspace");
    const customSkillsDir = join(root, "custom-skills");
    await mkdir(join(customSkillsDir, "demo-skill"), { recursive: true });
    await writeFile(
      join(customSkillsDir, "demo-skill", "SKILL.md"),
      "---\nname: demo-skill\ndescription: Demo skill for E2E verification\n---\n# Demo\n",
    );

    // Start HTTP server with holdMs = 6000ms so we can test both:
    //   1) a command exceeding WaitMsBeforeAsync (1s -> finishes at 2.5s <= 6s holdMs) returning in ONE call, and
    //   2) a command exceeding holdMs (finishes at 8.5s > 6s holdMs) returning a TaskId and completing via manage_task(status).
    const ownerToken = "test-owner-password-at-least-16-chars";
    const baseServerConfig: ServerConfig = {
      configDir: root,
      stateDir,
      host: "127.0.0.1",
      port: 0,
      listenPort: 0,
      publicBaseUrl: "http://127.0.0.1:19999",
      allowedHosts: ["127.0.0.1", "localhost"],
      oauth: {
        ownerToken,
        accessTokenTtlSeconds: 3600,
        refreshTokenTtlSeconds: 86400,
        refreshTokenGraceSeconds: 300,
        scopes: ["devspace"],
        allowedResourceUrls: [],
        allowedRedirectHosts: ["localhost", "127.0.0.1"],
      },
      agy: {
        bin: DEFAULT_AGY_BIN,
        workspace,
        agyHome,
        modelId: "gemini-3.1-pro-preview",
        wireProtocol: "genai",
        holdMs: 5_500,
        callTimeoutMs: 25_000,
        maxTurnsPerSession: 80,
        sessionIdleTtlMs: 600_000,
        exposedTools: ["read_file", "write_file", "edit_file", "run_command", "manage_task"],
      },
      skills: {
        enabled: true,
        paths: [customSkillsDir],
      },
      codeMode: {
        enabled: true,
      },
      ui: {
        enabled: true,
      },
      activity: {
        enabled: true,
        maxCalls: 200,
      },
      logging: {
        level: "error",
        format: "json",
        requests: false,
        assets: false,
        toolCalls: false,
        shellCommands: false,
        trustProxy: false,
      },
    };

    // Bind ephemeral port first so publicBaseUrl matches exact MCP resource URL
    const probeListener = (await import("node:http")).createServer();
    await new Promise<void>((r) => probeListener.listen(0, "127.0.0.1", r));
    const port = (probeListener.address() as AddressInfo).port;
    await new Promise<void>((r) => probeListener.close(() => r()));

    baseServerConfig.port = port;
    baseServerConfig.listenPort = port;
    baseServerConfig.publicBaseUrl = `http://127.0.0.1:${port}`;

    // Issue a real OAuth access token in stateDir before starting server
    const mcpUrl = new URL("/mcp", baseServerConfig.publicBaseUrl);
    const seedProvider = new SingleUserOAuthProvider(baseServerConfig.oauth, mcpUrl, stateDir);
    const oauthClient = await seedProvider.clientsStore.registerClient!({
      redirect_uris: ["http://127.0.0.1/callback"],
      client_name: "Gemini Task E2E",
    });
    seedProvider["codes"].set("seed-code", {
      clientId: oauthClient.client_id,
      params: {
        redirectUri: "http://127.0.0.1/callback",
        codeChallenge: "c",
        scopes: ["devspace"],
        resource: mcpUrl,
      },
      expiresAtMs: Date.now() + 60_000,
    });
    const tokens = await seedProvider.exchangeAuthorizationCode(
      oauthClient,
      "seed-code",
      undefined,
      "http://127.0.0.1/callback",
      mcpUrl,
    );
    seedProvider.close();

    const running = await createServer(baseServerConfig);
    const httpServer = running.app.listen(port, "127.0.0.1");
    await new Promise<void>((r) => httpServer.once("listening", r));
    t.after(async () => {
      await new Promise<void>((r) => httpServer.close(() => r()));
      await running.close();
    });

    const authHeaders = { Authorization: `Bearer ${tokens.access_token}` };

    // Connect two independent MCP clients (simulating two Gemini Task Sub Agents)
    const transportA = new StreamableHTTPClientTransport(mcpUrl, {
      requestInit: { headers: authHeaders },
    });
    const transportB = new StreamableHTTPClientTransport(mcpUrl, {
      requestInit: { headers: authHeaders },
    });
    const subAgentA = new Client({ name: "gemini-subagent-a", version: "1.0.0" });
    const subAgentB = new Client({ name: "gemini-subagent-b", version: "1.0.0" });

    await Promise.all([subAgentA.connect(transportA), subAgentB.connect(transportB)]);
    t.after(async () => {
      await subAgentA.close();
      await subAgentB.close();
    });

    // 1. Verify each Sub Agent got its own distinct Mcp-Session-Id
    assert.ok(transportA.sessionId, "Sub Agent A received Mcp-Session-Id");
    assert.ok(transportB.sessionId, "Sub Agent B received Mcp-Session-Id");
    assert.notEqual(transportA.sessionId, transportB.sessionId);

    // 2. Verify exposed tools (5 agy + 3 Code Mode) and cleaned JSON schemas + skills instructions
    assert.match(subAgentA.getInstructions() ?? "", /demo-skill/);
    const { tools } = await subAgentA.listTools();
    assert.deepEqual(
      tools.map((tool) => tool.name).sort(),
      [
        "cancel_execution",
        "edit_file",
        "execute",
        "manage_task",
        "poll_execution",
        "read_file",
        "run_command",
        "write_file",
      ],
    );
    for (const tool of tools) {
      assert.equal("$schema" in tool.inputSchema, false, `${tool.name} schema clean`);
    }
    const readTool = tools.find((tool) => tool.name === "read_file")!;
    assert.match(readTool.description ?? "", /image/i);

    // 3. Verify write_file -> edit_file -> read_file (text + PNG image)
    const sampleTxt = join(workspace, "hello.txt");
    await subAgentA.callTool({
      name: "write_file",
      arguments: { Path: sampleTxt, Content: "alpha\nbeta\n", Overwrite: true },
    });
    await subAgentA.callTool({
      name: "edit_file",
      arguments: { Path: sampleTxt, Target: "beta", Replacement: "gamma" },
    });
    const readTxtRes = await subAgentA.callTool({
      name: "read_file",
      arguments: { Path: sampleTxt },
    });
    const readTxt = (readTxtRes.content as Array<{ type: string; text?: string }>)[0]?.text ?? "";
    assert.match(readTxt, /alpha/);
    assert.match(readTxt, /gamma/);

    const samplePng = join(workspace, "pixel.png");
    await writeFile(samplePng, Buffer.from(TINY_PNG_BASE64, "base64"));
    const readPngRes = await subAgentA.callTool({
      name: "read_file",
      arguments: { Path: samplePng },
    });
    const imgBlock = (
      readPngRes.content as Array<{ type: string; mimeType?: string; data?: string }>
    ).find((c) => c.type === "image");
    assert.ok(imgBlock, "read_file on PNG returned an MCP image content block");
    assert.equal(imgBlock.mimeType, "image/png");
    assert.equal(imgBlock.data, TINY_PNG_BASE64);

    // 4. Verify true parallel execution across Sub Agent A and Sub Agent B
    const parallelStart = Date.now();
    const [resA, resB] = await Promise.all([
      subAgentA.callTool({
        name: "run_command",
        arguments: {
          CommandLine: "sleep 1.2 && echo SUB_A_OK",
          WaitMsBeforeAsync: 5000,
          toolSummary: "Parallel A",
        },
      }),
      subAgentB.callTool({
        name: "run_command",
        arguments: {
          CommandLine: "sleep 1.2 && echo SUB_B_OK",
          WaitMsBeforeAsync: 5000,
          toolSummary: "Parallel B",
        },
      }),
    ]);
    const parallelElapsed = Date.now() - parallelStart;
    assert.ok(
      parallelElapsed < 2_300,
      `Expected parallel execution (<2300ms), got ${parallelElapsed}ms`,
    );
    assert.match(
      (resA.content as Array<{ text?: string }>)[0]?.text ?? "",
      /SUB_A_OK/,
    );
    assert.match(
      (resB.content as Array<{ text?: string }>)[0]?.text ?? "",
      /SUB_B_OK/,
    );

    // 5. Verify server-side hold on run_command: WaitMsBeforeAsync=600 backgrounds at 0.6s in agy,
    //    command finishes at 2.0s (<= holdMs 5.5s), and returns synchronously in ONE call!
    const heldCmdRes = await subAgentA.callTool({
      name: "run_command",
      arguments: {
        CommandLine: "sleep 2.0 && echo HELD_ONE_SHOT_DONE",
        WaitMsBeforeAsync: 600,
        toolSummary: "Held sync command",
      },
    });
    const heldText = (heldCmdRes.content as Array<{ text?: string }>)[0]?.text ?? "";
    assert.match(heldText, /HELD_ONE_SHOT_DONE/);

    // 6. Verify command exceeding holdMs (sleep 7.5s > holdMs 5.5s) returns TaskId,
    //    Sub Agent B does NOT see Sub Agent A's task, and manage_task(status) holds until completion
    //    AND preserves the real exit code (42) from <SYSTEM_MESSAGE>!
    const longCmdRes = await subAgentA.callTool({
      name: "run_command",
      arguments: {
        CommandLine: "sleep 7.5 && echo LONG_ASYNC_DONE && exit 42",
        WaitMsBeforeAsync: 600,
        toolSummary: "Long async command",
      },
    });
    const longText = (longCmdRes.content as Array<{ text?: string }>)[0]?.text ?? "";
    const taskIdMatch = longText.match(/task id: (\S+)/i);
    assert.ok(taskIdMatch, `Expected background task id in: ${longText}`);
    const taskId = taskIdMatch[1]!;

    // Sub Agent B's task list must NOT contain Sub Agent A's running task
    const listBRes = await subAgentB.callTool({
      name: "manage_task",
      arguments: { Action: "list" },
    });
    const listBText = (listBRes.content as Array<{ text?: string }>)[0]?.text ?? "";
    assert.equal(listBText.includes(taskId), false, "Sub Agent B must not see Sub Agent A's task");

    // Sub Agent A calls manage_task(status) which holds until the 7.5s command completes
    const statusRes = await subAgentA.callTool({
      name: "manage_task",
      arguments: { Action: "status", TaskId: taskId },
    });
    const statusText = (statusRes.content as Array<{ text?: string }>)[0]?.text ?? "";
    assert.match(statusText, /LONG_ASYNC_DONE/);
    assert.match(statusText, /code 42/);

    // 7. Verify Code Mode (execute batching multiple agy calls + poll_execution)
    const batchFile = join(workspace, "codemode.txt");
    const execRes = await subAgentA.callTool({
      name: "execute",
      arguments: {
        code: `
          await tools.write_file({ Path: ${JSON.stringify(batchFile)}, Content: "line1\\nline2\\n", Overwrite: true });
          const r = await tools.read_file({ Path: ${JSON.stringify(batchFile)} });
          console.log("batch ok");
          return { containsLine2: r.text.includes("line2") };
        `,
      },
    });
    const execSnap = JSON.parse((execRes.content as Array<{ text?: string }>)[0]?.text ?? "{}") as {
      status: string;
      result?: { containsLine2?: boolean };
      calls?: Array<{ tool: string }>;
      logs?: string[];
    };
    assert.equal(execSnap.status, "completed");
    assert.equal(execSnap.result?.containsLine2, true);
    assert.equal(execSnap.calls?.length, 2);

    const asyncExecRes = await subAgentA.callTool({
      name: "execute",
      arguments: {
        code: `await sleep(1200); return "polled-ok";`,
        yield_time_ms: 500,
      },
    });
    const asyncSnap = JSON.parse((asyncExecRes.content as Array<{ text?: string }>)[0]?.text ?? "{}") as {
      execution_id: string;
      status: string;
    };
    assert.equal(asyncSnap.status, "running");
    const polledRes = await subAgentA.callTool({
      name: "poll_execution",
      arguments: { execution_id: asyncSnap.execution_id, yield_time_ms: 3000 },
    });
    const polledSnap = JSON.parse((polledRes.content as Array<{ text?: string }>)[0]?.text ?? "{}") as {
      status: string;
      result?: string;
    };
    assert.equal(polledSnap.status, "completed");
    assert.equal(polledSnap.result, "polled-ok");

    // 8. Verify /web Activity UI auth + recorded session calls
    const unauthWeb = await fetch(`${baseServerConfig.publicBaseUrl}/web/api/calls`);
    assert.equal(unauthWeb.status, 401);

    const loginRes = await fetch(`${baseServerConfig.publicBaseUrl}/web/login`, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ owner_token: ownerToken }),
      redirect: "manual",
    });
    assert.equal(loginRes.status, 303);
    const cookie = loginRes.headers.get("set-cookie")?.split(";")[0] ?? "";
    assert.match(cookie, /^devspace_web_session=/);

    const authWeb = await fetch(`${baseServerConfig.publicBaseUrl}/web/api/calls`, {
      headers: { cookie },
    });
    assert.equal(authWeb.status, 200);
    const webData = (await authWeb.json()) as {
      sessions: Array<{ mcpSessionId: string }>;
      calls: Array<{ tool: string }>;
    };
    assert.ok(webData.sessions.length >= 2);
    assert.ok(webData.calls.some((c) => c.tool === "execute"));

    // 9. Verify stale/restarted Mcp-Session-Id header never returns 400 or 404
    const staleResp = await fetch(mcpUrl, {
      method: "POST",
      headers: {
        ...authHeaders,
        "content-type": "application/json",
        accept: "application/json, text/event-stream",
        "mcp-session-id": "restarted-stale-session-id-9999",
        "mcp-protocol-version": "2025-11-25",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 77,
        method: "tools/list",
        params: {},
      }),
    });
    assert.equal(staleResp.status, 200);
  },
);
