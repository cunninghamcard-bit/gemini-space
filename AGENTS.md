# Repository Guide

- **Build**: `pnpm build`
- **Typecheck**: `pnpm typecheck`
- **Test**: `pnpm test` (uses Node's built-in `node:sqlite` and `node:test`)
- **Architecture**:
  - `src/agy-tool-schemas.ts`, `src/agy-file-tools.ts`, `src/agy-command-runner.ts`, `src/agy-bridge.ts`: pure Node.js deterministic implementation of Antigravity (`agy`) tools (`read_file`, `write_file`, `edit_file`, `run_command`, `manage_task`) with per-`Mcp-Session-Id` isolation.
  - `src/code-mode.ts`: async JavaScript batch execution (`execute`, `poll_execution`, `cancel_execution`).
  - `src/skills.ts`: automatic `SKILL.md` discovery injected into MCP server `instructions`.
  - `src/schema-clean.ts`: strips JSON Schema draft keywords rejected by Google Gemini Web Connectors.
  - `src/oauth-provider.ts` & `src/oauth-store.ts`: single-user OAuth 2.1 with SQLite hashed token persistence and refresh-token rotation grace.
  - `src/activity-web.ts`: owner-password protected `/web/` audit UI.
