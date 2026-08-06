# Agent indexing — CodeGraph + GitNexus

Local-first code intelligence for Cursor (and other MCP agents). Indexes stay on disk
and are **not** committed (see `.gitignore`: `.codegraph/`, `.gitnexus/`).

Canonical human maps live beside this file:

| Doc | Role |
|-----|------|
| [architecture.md](architecture.md) | Engine design (Loom / HVM / Connect) |
| [CODEMAP.md](CODEMAP.md) | Directory → ownership / entrypoints |
| [TODOS.md](TODOS.md) | Open work queue with file pointers |
| [NEXT_STEPS.md](NEXT_STEPS.md) | Resume guide / phase narrative |
| [ISSUES.md](ISSUES.md) | Issue-level progress history |

The private control plane (`gitlab.com/weftlabs/weft-platform`) mirrors ARCHITECTURE /
CODEMAP / TODOS / AGENT_INDEXING under its `docs/`.

## One-time install (developer machine)

```sh
# CodeGraph — CLI + Cursor MCP wiring
npm i -g @colbymchenry/codegraph
# or: curl -fsSL https://raw.githubusercontent.com/colbymchenry/codegraph/main/install.sh | sh
codegraph install --target=cursor --yes

# GitNexus — CLI + Cursor MCP wiring
npm i -g gitnexus
gitnexus setup -c cursor
```

Restart Cursor after MCP config changes.

## Index each repo

From this repo root (and separately from the platform checkout):

```sh
./scripts/index-agents.sh
# equivalent:
codegraph init
gitnexus analyze --skip-agents-md   # preserve AGENTS.md Cursor Cloud section
```

Verify:

```sh
codegraph status
gitnexus doctor
git check-ignore -v .codegraph .gitnexus
```

## Multi-repo group (`oxidant`)

After **both** this repo and `oxidant-platform` are analyzed:

```sh
gitnexus group create oxidant
gitnexus group add oxidant engine /absolute/path/to/oxidant
gitnexus group add oxidant platform /absolute/path/to/oxidant-platform
gitnexus group sync oxidant
gitnexus group status oxidant
```

Use group tools when work spans the Spark Connect / governance boundary that platform
depends on via git deps (see platform `Cargo.toml` / `docs/runtime-contract.md` here).

## How agents should use the tools

| Tooling | Prefer for |
|---------|------------|
| **CodeGraph** (`codegraph_explore`) | Surgical “how does X work?”, call paths, blast radius with source |
| **GitNexus** (`query`, `context`, `impact`, `trace`, `detect_changes`) | Process/cluster view, edit impact |
| **GitNexus group_*** | Cross-repo platform ↔ engine impact |
| **Committed docs/** | Task intake: `TODOS.md` → `CODEMAP.md` → `architecture.md` |

If indexes are missing, use committed docs and normal search — do not block on indexing.

## Re-index after large pulls

```sh
./scripts/index-agents.sh
gitnexus group sync oxidant   # if you use the multi-repo group
```
