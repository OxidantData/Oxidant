# Durable query history and exec logs

Status: **design** (2026-08-24). Follow-up to issue #134 (Connect executions must
join the statements store) and the founder ask: *server exec logs durable, rolled
daily/hourly/weekly, max lookback 30 days*.

## 0. TL;DR

The engine keeps its query history and its process logs on **disk, in append-only
files it writes itself** — no embedded database, no new dependency. Statements are a
JSONL journal that is replayed at boot; results are Arrow IPC files referenced by
statement id; exec logs roll on a clock with a 30-day prune. Everything has a cap,
everything has an honest failure mode, and nothing a query needs ever waits on
history.

## 1. Today: history is volatile

| Store | Where | Persistence |
|---|---|---|
| `StatementStore` (the statements rail) | `crates/oxidant-connect/src/rest.rs` | **none** — in-memory, 1000 cap, wiped on restart |
| `AppStateStore` (jobs/stages/sql) | `crates/oxidant-observability/src/store.rs` | optional `event_log_dir` JSONL, off by default |
| `LogBuffer` (recent exec log lines) | `crates/oxidant-connect/src/rest.rs` | **none** — 1000-line ring |
| driver `/api/status` last-queries | `oxidant-execution` | **none** |

Consequences a restart brings today: the statements rail empties, running statements
leave no trace, results are unrecoverable, and process logs vanish. The platform
masks this for its own runs (`query_history` in the platform DB, written at submit),
but a direct engine user — PySpark, REST, the embedded UI — sees nothing.

## 2. Goals and non-goals

**Goals.** (1) Every execution — REST *and* Spark Connect, closing #134's
persistence half — is recorded durably with provenance. (2) Statement results
survive a restart within an explicit size budget. (3) Exec logs are durable and
rolled daily/hourly/weekly with a 30-day max lookback. (4) Zero new crate
dependencies (the engine's dependency culture is deliberate: `protox` instead of
`protoc`). (5) Replay at boot is bounded. (6) A slow or full disk degrades history,
never execution.

**Non-goals.** SQL-over-history (the platform's `query_history` does that, and
syncing engine→platform stays the platform's reconcile path), cross-node shared
history (each engine owns its dir; a driver/worker pair each keeps its own),
results beyond the size budget (the CSV/live path is for that), log shipping
(stdout stays the contract for collectors).

## 3. On-disk layout

```
$OXIDANT_DATA_DIR/                  # default ./oxidant-data next to checkpoints
  history/
    statements/
      seg-000042.jsonl              # append-only lifecycle events, rolled at 64 MiB
      compacted/                    # compaction output, atomically swapped in
    results/
      <statement-id>.arrow          # Arrow IPC stream, one file per statement
  logs/
    oxidant.log                     # the current file
    oxidant-2026-08-23.parquet      # rolled + compressed (zstd) — see §6
    oxidant-2026-08-23-14.parquet   # when OXIDANT_LOG_ROLL=hourly
    oxidant-2026-W34.parquet        # when OXIDANT_LOG_ROLL=weekly
```

**Disk guards (hard ceilings — logs must never fill the server).** Everything under
`$OXIDANT_DATA_DIR` lives under one budget and a free-space floor; the engine
deletes the oldest thing it owns before it lets the disk fill:

| Knob | Default | Meaning |
|---|---|---|
| `OXIDANT_DISK_MAX_BYTES` | 8 GiB | total budget for `history/` + `logs/` + `results/` combined |
| `OXIDANT_DISK_MIN_FREE_BYTES` | 1 GiB | filesystem free-space floor — prune aggressively below it regardless of retention days |
| `OXIDANT_LOG_MAX_FILE_BYTES` | 256 MiB | the live log rotates **early** at this size, even mid-period |
| `OXIDANT_LOG_MAX_TOTAL_BYTES` | 2 GiB | logs/ subtree cap — oldest rolled files deleted first |
| `OXIDANT_LOG_DEDUP` | on | a repeated identical line collapses to `… repeated N times` (the classic rsyslog guard against a hot loop filling the disk) |

Prune order when over budget: oldest rolled logs → oldest result files → oldest
journal segments. The live log file is **never** deleted — it rotates instead. If
the budget is still exceeded after everything prunable is gone, `/api/status`
reports `disk: over_budget` (alongside `history_writes: degraded`), the engine
keeps serving, and the log carries one loud line per prune pass naming what was
removed and why. The sweeper runs at roll time, at boot, and every 5 minutes.

Knobs (runtime-contract documented): `OXIDANT_HISTORY_DIR`,
`OXIDANT_HISTORY=on|off` (default on), `OXIDANT_HISTORY_FSYNC=statement|interval`
(default `statement`, interval 500 ms), `OXIDANT_HISTORY_MAX_STATEMENTS`
(default 10,000), `OXIDANT_HISTORY_RETENTION_DAYS` (default **30**),
`OXIDANT_RESULT_PERSIST=on_pressure|always|never` (default `on_pressure`),
`OXIDANT_RESULT_MAX_BYTES` (default 256 MiB per file),
`OXIDANT_LOG_ROLL=daily|hourly|weekly` (default `daily`),
`OXIDANT_LOG_KEEP_DAYS` (default **30**).

## 4. The statement journal

One JSON line per lifecycle event:

```json
{"v":1,"kind":"submitted","id":"stmt-8812","sql":"SELECT …","source":"connect",
 "session":"0011…","ts":"2026-08-23T18:02:11.004Z"}
{"v":1,"kind":"finished","id":"stmt-8812","state":"finished","rows":12,
 "duration_ms":143,"ts":"…"}
```

- `kind`: `submitted | running | finished | failed | cancelled`.
- `source`: `rest | connect` — Connect `ExecutePlan` submits with `connect`,
  which is what unifies the history for issue #134. The journal entry is written
  **at submit**, so a crash mid-statement still leaves a trace.
- **Write ordering**: append on a dedicated writer task (bounded channel). `fsync`
  when a terminal event is durably the answer a client already received; lifecycle
  chatter in between flushes on the 500 ms interval. After a crash you can lose up
  to one interval of *intermediate* events — never a terminal state that was
  acknowledged.
- **Replay**: fold events into the state map (last event wins). Statements left
  non-terminal at shutdown are marked `interrupted` — the same honesty the
  platform's boot reconcile practices. A corrupt tail stops replay at the first
  bad line, is renamed `seg-…jsonl.corrupt`, and boot continues; history must never
  be the reason the engine does not start.
- **Compaction**: a background pass rewrites segments keeping only the terminal
  state per statement once the superseded ratio passes 50%; output swaps in with
  write-tmp-then-rename. Retention drops everything past
  `OXIDANT_HISTORY_RETENTION_DAYS` and beyond `MAX_STATEMENTS` (oldest terminal
  first — a running statement is never evicted).

## 5. Result retention

- Terminal results (already retained in memory today) are written to
  `results/<id>.arrow` when the in-memory budget pressures out (`on_pressure`) or
  always (`always`). Files larger than `OXIDANT_RESULT_MAX_BYTES` are refused and
  the statement records `result_too_large` — the live/CSV path is the answer past
  the budget, stated plainly.
- `/api/v1/statements/{id}/result` reads memory → falls back to disk → answers
  `410 result_expired` when both are gone. The error vocabulary does not change;
  it just stops meaning "the process restarted".

## 6. Rolling exec logs, compressed

- A small in-tree rolling writer behind the existing `tracing` layer
  (`LogBuffer` stays as the live in-memory tail; the file is the durable copy).
- **Two roll triggers, whichever first**: the clock boundary (`daily` default,
  `hourly`/`weekly` via `OXIDANT_LOG_ROLL`) or the size cap
  (`OXIDANT_LOG_MAX_FILE_BYTES`) — a chatty hour rotates early instead of growing
  without bound.
- **On roll, the closed file is converted to Parquet (zstd)** — schema
  `(ts, level, target, message, fields_json)` using the Arrow/Parquet stack the
  engine already ships (no new dependency). Text logs compress roughly 10×, so a
  day of verbose engine logs lands in tens of MiB. The raw text file is removed
  **only after** the Parquet file's footer is read back successfully — a failed
  conversion keeps the text file and says so.
- **Repeated-line suppression** (`OXIDANT_LOG_DEDUP`): an identical consecutive
  line is counted and flushed as `… repeated N times` on change or every 60 s —
  a hot error loop cannot fill the disk between two sweeps.
- Retention: `OXIDANT_LOG_KEEP_DAYS` (default **30**) plus the hard guards in §3 —
  the size budget can prune a file before its 30 days are up, and says so in the
  log when it does.
- `GET /api/v1/logs` gains `?file=current|YYYY-MM-DD[‑HH]`: `current` reads the
  live text file; a rolled date reads the Parquet back through the engine's own
  Parquet reader (same rows the text file had). Absent file → 404, same honesty
  as the connector-log endpoint.

## 7. Failure semantics (the honesty section)

- **A query never waits on history.** The writer channel is bounded; when full,
  intermediate events coalesce (terminal always kept). A stalled disk cannot stall
  a statement.
- **Crash window**: ≤ one fsync interval of intermediate lifecycle events. What a
  client was told is always durable first.
- **ENOSPC / EIO**: appends start failing → `/api/status` reports
  `history_writes: degraded`; the engine keeps executing. Recovered disk flips it
  back without a restart.
- **Corruption**: quarantine-and-continue (§4). Boot is never blocked by history.

## 8. Compatibility

- Default **on** with the caps above; `OXIDANT_HISTORY=off` restores today's
  behaviour exactly.
- `event_log_dir` stays untouched — it is the Spark-history-server compatibility
  surface; the journal is deliberately separate so each can evolve on its own
  contract.

## 9. Test plan

- crash-during-append → replay loses ≤ the interval and marks non-terminals
  `interrupted`;
- corrupt tail → quarantined, boot succeeds, earlier statements intact;
- compaction preserves exactly the terminal states;
- retention: 30-day prune and MAX_STATEMENTS eviction never touch a running
  statement;
- result spill → process restart → `/result` still answers; oversized result →
  `result_too_large`;
- log roll across a fake clock at daily/hourly/weekly boundaries, 30-day prune;
- the size roll fires mid-period at `OXIDANT_LOG_MAX_FILE_BYTES`;
- rolled text → Parquet round-trip returns the same rows, and a failed
  conversion keeps the text file;
- the dedup guard collapses a hot loop into `… repeated N times`;
- the disk-budget sweeper prunes in the documented order, never touches the live
  file, and reports `over_budget` only after everything prunable is gone;
- a Connect-submitted statement replays with `source: "connect"` (the #134 pin);
- degraded mode under a failing-writer shim: statements execute, status reports
  `degraded`, recovery is automatic.

## 10. Rollout

1. **PR1** — the journal + replay + Connect unification (closes #134's durable half).
2. **PR2** — result spill + disk fallback in `/result`.
3. **PR3** — rolling exec logs + `?file=` on `/api/v1/logs`.
