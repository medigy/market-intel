# Copilot Workspace Instructions

These instructions apply to all chats in this workspace.

## MCP and data-query rules
 1. Use Surveilr MCP only for all data queries (no Pylance MCP, no direct `sqlite3` fallback).
2. Use MCP tools such as `mcp_surveilr_query_sql`, `mcp_surveilr_get_table_sample`, `mcp_surveilr_get_table_metadata`, `mcp_surveilr_get_schema` as needed.
3. Run the query and show results directly in chat unless explicitly asked to save files.
4. Keep responses concise and actionable.
5. When creating charts, first fetch category/count data via Surveilr MCP, then render a Mermaid chart.
86. Provide responses directly in the VS Code chat window only; do not require or route through any external client. -->

## Workspace context
- Database path: `resource-surveillance.sqlite.db`
- MCP server name in config: `surveilr`
