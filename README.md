# Gemini Space

Self-hosted MCP server tailored for **Google Gemini Web (including Task multi-subagent mode)** on macOS.

## Features

- **5 Native Deterministic Antigravity (`agy`) Tools**: `read_file` (multimodal text/image/PDF/audio/video), `write_file`, `edit_file`, `run_command` (80×24 PTY with 45s synchronous completion hold), and `manage_task` (45s server-side status hold, `send_input`, process-tree `kill`).
- **Multi-Subagent Session Isolation**: Issues soft `Mcp-Session-Id` headers on `initialize` so parallel Gemini Task subagents execute concurrently with isolated background task queues and restart-resilient fallback.
- **Code Mode (`execute` / `poll_execution` / `cancel_execution`)**: Batch multiple local file and command operations in a single async JS round trip.
- **Local Skills Discovery**: Automatically scans `~/.agents/skills`, `~/.claude/skills`, `~/.codex/skills`, and `~/.devspace/skills` into MCP server instructions.
- **OAuth 2.1 + `/web/` Activity Dashboard**: Single-owner password approval, SQLite token persistence with 300s refresh-token rotation grace, built-in Gemini Connector JSON Schema sanitization, and an owner-protected `/web/` session audit UI.

## Quick Commands

```bash
pnpm build
node --import ./bin/silence-sqlite-warning.js dist/cli.js doctor
node --import ./bin/silence-sqlite-warning.js dist/cli.js serve
```
