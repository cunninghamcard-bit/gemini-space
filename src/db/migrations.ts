import { transaction, type SqliteDatabase } from "./sqlite.js";

/**
 * Idempotent OAuth-only schema setup.
 * Intentionally tolerates pre-existing `devspace_schema_migrations` rows written
 * by older DevSpace or Synrex builds so existing OAuth grants survive seamlessly.
 */
export function migrateDatabase(sqlite: SqliteDatabase): void {
  transaction(sqlite, () => {
    sqlite.exec(`
      create table if not exists devspace_schema_migrations (
        version integer primary key,
        name text not null,
        applied_at text not null
      );

      create table if not exists oauth_clients (
        client_id text primary key,
        client_json text not null,
        issued_at integer not null
      );

      create index if not exists oauth_clients_issued_at_idx
        on oauth_clients(issued_at desc);

      create table if not exists oauth_access_tokens (
        token_hash text primary key,
        client_id text not null,
        scopes_json text not null,
        expires_at integer not null,
        resource text,
        foreign key (client_id) references oauth_clients(client_id) on delete cascade
      );

      create index if not exists oauth_access_tokens_client_id_idx
        on oauth_access_tokens(client_id);

      create index if not exists oauth_access_tokens_expires_at_idx
        on oauth_access_tokens(expires_at);

      create table if not exists oauth_refresh_tokens (
        token_hash text primary key,
        client_id text not null,
        scopes_json text not null,
        expires_at integer not null,
        resource text,
        foreign key (client_id) references oauth_clients(client_id) on delete cascade
      );

      create index if not exists oauth_refresh_tokens_client_id_idx
        on oauth_refresh_tokens(client_id);

      create index if not exists oauth_refresh_tokens_expires_at_idx
        on oauth_refresh_tokens(expires_at);

      create table if not exists mcp_activity_calls (
        id integer primary key autoincrement,
        created_at text not null,
        mcp_session_id text not null,
        tool text not null,
        args_json text not null,
        output_preview text not null,
        duration_ms integer not null,
        is_error integer not null default 0
      );

      create index if not exists mcp_activity_calls_session_idx
        on mcp_activity_calls(mcp_session_id, id desc);
    `);

    const hasOAuthMigration = sqlite
      .prepare("select 1 from devspace_schema_migrations where version = 2")
      .get();

    if (!hasOAuthMigration) {
      sqlite
        .prepare("insert into devspace_schema_migrations (version, name, applied_at) values (?, ?, ?)")
        .run(2, "oauth-state", new Date().toISOString());
    }
  }, "immediate");
}
