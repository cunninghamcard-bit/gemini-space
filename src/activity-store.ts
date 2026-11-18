import { openDatabase, type DatabaseHandle } from "./db/client.js";

export interface ActivityCallRecord {
  id: number;
  createdAt: string;
  mcpSessionId: string;
  tool: string;
  argsJson: string;
  outputPreview: string;
  durationMs: number;
  isError: boolean;
}

export interface ActivitySessionSummary {
  mcpSessionId: string;
  callCount: number;
  errorCount: number;
  lastCallAt: string;
}

const MAX_ARGS_CHARS = 8_000;
const MAX_OUTPUT_CHARS = 8_000;

export class ActivityStore {
  private readonly database: DatabaseHandle;

  constructor(
    stateDir: string,
    private readonly maxCalls = 500,
  ) {
    this.database = openDatabase(stateDir);
  }

  recordCall(entry: {
    mcpSessionId: string | undefined;
    tool: string;
    args: Record<string, unknown> | undefined;
    outputText: string;
    durationMs: number;
    isError: boolean;
  }): void {
    const sessionId = entry.mcpSessionId?.trim() || "stateless";
    const createdAt = new Date().toISOString();
    let argsJson = "{}";
    try {
      argsJson = JSON.stringify(entry.args ?? {}).slice(0, MAX_ARGS_CHARS);
    } catch {
      argsJson = "{}";
    }
    const outputPreview = entry.outputText.slice(0, MAX_OUTPUT_CHARS);

    this.database.sqlite
      .prepare(
        `insert into mcp_activity_calls
          (created_at, mcp_session_id, tool, args_json, output_preview, duration_ms, is_error)
         values (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        createdAt,
        sessionId,
        entry.tool,
        argsJson,
        outputPreview,
        Math.max(0, Math.round(entry.durationMs)),
        entry.isError ? 1 : 0,
      );

    // Keep table bounded to maxCalls rows
    this.database.sqlite
      .prepare(
        `delete from mcp_activity_calls
         where id not in (
           select id from mcp_activity_calls order by id desc limit ?
         )`,
      )
      .run(this.maxCalls);
  }

  listSessions(): ActivitySessionSummary[] {
    const rows = this.database.sqlite
      .prepare(
        `select
           mcp_session_id,
           count(*) as call_count,
           sum(is_error) as error_count,
           max(created_at) as last_call_at
         from mcp_activity_calls
         group by mcp_session_id
         order by last_call_at desc`,
      )
      .all() as Array<{
      mcp_session_id: string;
      call_count: number;
      error_count: number | null;
      last_call_at: string;
    }>;

    return rows.map((r) => ({
      mcpSessionId: r.mcp_session_id,
      callCount: Number(r.call_count),
      errorCount: Number(r.error_count ?? 0),
      lastCallAt: r.last_call_at,
    }));
  }

  listCalls(options: { mcpSessionId?: string; limit?: number } = {}): ActivityCallRecord[] {
    const limit = Math.max(1, Math.min(options.limit ?? 100, 500));
    const rows = options.mcpSessionId
      ? (this.database.sqlite
          .prepare(
            `select id, created_at, mcp_session_id, tool, args_json, output_preview, duration_ms, is_error
             from mcp_activity_calls
             where mcp_session_id = ?
             order by id desc
             limit ?`,
          )
          .all(options.mcpSessionId, limit) as Array<Record<string, unknown>>)
      : (this.database.sqlite
          .prepare(
            `select id, created_at, mcp_session_id, tool, args_json, output_preview, duration_ms, is_error
             from mcp_activity_calls
             order by id desc
             limit ?`,
          )
          .all(limit) as Array<Record<string, unknown>>);

    return rows.map((r) => ({
      id: Number(r.id),
      createdAt: String(r.created_at),
      mcpSessionId: String(r.mcp_session_id),
      tool: String(r.tool),
      argsJson: String(r.args_json),
      outputPreview: String(r.output_preview),
      durationMs: Number(r.duration_ms),
      isError: Number(r.is_error) !== 0,
    }));
  }

  close(): void {
    this.database.close();
  }
}
