# MCP server — `oxidant mcp`

[MCP](https://modelcontextprotocol.io) (Model Context Protocol) is an open protocol that lets
AI assistants call external tools. `oxidant mcp` exposes a running Oxidant server as MCP tools
over **stdio**, so Claude Desktop, Cursor, and other MCP clients can run SQL, inspect tables,
and check cluster state against your engine.

```text
oxidant mcp [--url http://localhost:4040]
```

The server URL comes from `--url` or the `OXIDANT_URL` environment variable (default
`http://localhost:4040`). Start the engine first (`oxidant start --port 50051`), then
register `oxidant mcp` with your MCP client — the client launches it as a subprocess and talks
to it over stdio.

## Claude Desktop

Add to `claude_desktop_config.json` (macOS:
`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "oxidant": {
      "command": "oxidant",
      "args": ["mcp"],
      "env": {
        "OXIDANT_URL": "http://localhost:4040"
      }
    }
  }
}
```

Restart Claude Desktop after saving. If `oxidant` is not on your login `PATH`, use the absolute
binary path as `command`.

## Cursor

Add to `.cursor/mcp.json` in your project (or `~/.cursor/mcp.json` globally):

```json
{
  "mcpServers": {
    "oxidant": {
      "command": "oxidant",
      "args": ["mcp"],
      "env": {
        "OXIDANT_URL": "http://localhost:4040"
      }
    }
  }
}
```

## Tools

| Tool | What it does |
|------|--------------|
| `run_sql` | Execute a SQL statement and return the result rows |
| `statement_status` | Poll a submitted statement's status (`pending`/`running`/`succeeded`/`failed`/`canceled`, plus error/rowCount/schema) |
| `list_statements` | List recent statements, newest first |
| `cancel_statement` | Cancel a pending or running statement |
| `cluster_status` | Cluster mode (`single-node`/`local-cluster`/`distributed`), workers, engine version |
| `list_tables` | List visible tables (including external catalogs such as Glue) |
| `describe_table` | Return a table's schema (column names and types) |

All tools are thin wrappers over the [REST API](api.md), so anything the agent does also shows
up in the Web UI's statement list.

## Example prompts

Once the server is registered, ask the assistant things like:

- "List the tables available in Oxidant, then describe the schema of `glue.oxidant_demo.orders`."
- "Run `SELECT count(*) FROM glue.oxidant_demo.orders` and tell me the row count."
- "That last query is taking too long — check its status and cancel it if it's still running."
- "Check the cluster status: are we single-node or distributed, and how many workers are attached?"

## Notes

- **No auth.** Like the REST API it wraps, `oxidant mcp` has no authentication — point it only
  at servers you trust, and keep the UI port bound to loopback on shared hosts
  (`--ui-bind 127.0.0.1`).
- Agents can run any SQL the engine accepts, including DDL/CTAS. Review tool calls if the
  server points at real data.
