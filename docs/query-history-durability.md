# Durable query history and exec logs

Status: **PR1–PR3 built, PR4 designed** (2026-08-24, rev 2 after design review; §10 tracks
what has shipped). Follow-up to issue #134
(Connect executions must join the statements store) and the founder ask: *server exec
logs durable, rolled daily/hourly/weekly, max lookback 30 days*.

Rev 2 resolves the review findings F1–F21; §11 maps each finding to its resolution.

## 0. TL;DR

The engine keeps its query history and its process logs on **disk, in append-only
files it writes itself** — no embedded database, no new third-party dependency.
Statements are a JSONL journal of **self-contained records** that is replayed at boot;
results are Arrow IPC files referenced by an engine-minted statement id; exec logs roll
on a UTC clock (and on size) with a 30-day prune. Everything has a cap, everything has
an honest failure mode, and **execution never waits on history** — the *response* may,
briefly and boundedly, so that what a client is told is true.

## 1. Today: history is volatile

| Store | Where | Persistence |
|---|---|---|
| `StatementStore` (the statements rail) | `crates/oxidant-connect/src/rest.rs` | **none** — in-memory, 1000 cap, 1 h TTL, wiped on restart |
| `AppStateStore` (jobs/stages/sql) | `crates/oxidant-observability/src/store.rs` | optional `event_log_dir` JSONL, off by default, **never rolled or pruned** |
| `LogBuffer` (recent exec log lines) | `crates/oxidant-connect/src/rest.rs` | **none** — 1000-line ring |
| driver `/api/status` last-queries | `oxidant-execution` | **none** |

Consequences a restart brings today: the statements rail empties, running statements
leave no trace, results are unrecoverable, and process logs vanish. The platform
masks this for its own runs (`query_history` in the platform DB, written at submit),
but a direct engine user — PySpark, REST, the embedded UI — sees nothing.

Two facts from that code shape the whole design and are called out where they bite:
`StoreInner::evict_expired` runs on **every** insert against a 1 h TTL (§5b), and
Connect's `operation_id` is **client-supplied** and unvalidated (§4b).

## 2. Goals and non-goals

**Goals.** (1) Every execution — REST *and* Spark Connect, closing #134's
persistence half — is recorded durably with provenance. (2) Statement results
survive a restart within an explicit size budget. (3) Exec logs are durable and
rolled daily/hourly/weekly with a 30-day max lookback, on the driver **and** on
every worker process. (4) Zero new crate dependencies (the engine's dependency
culture is deliberate: `protox` instead of `protoc`). (5) Replay at boot is bounded.
(6) A slow or full disk degrades history, never execution.

**Non-goals.** SQL-over-history (the platform's `query_history` does that, and
syncing engine→platform stays the platform's reconcile path), cross-node shared
history (each node owns its dir; the driver *federates reads*, §6b, it does not
ingest), results beyond the size budget (the CSV/live path is for that), log shipping
(stdout stays the contract for collectors).

**On Goal 4.** Nothing third-party is added. Two workspace-internal moves are needed
and are in scope: `chrono` (already in `oxidant-observability`, `oxidant-cli`,
`oxidant-streaming`, `oxidant-pipelines`, `oxidant-loom`, `oxidant-datasource`) gains
an `oxidant-connect` entry with `features = ["std", "clock"]` — the `oxidant-loom` pin
is `default-features = false, features = ["std"]` and has no clock, so the feature set
must be spelled out; and `sysinfo` (already `oxidant-connect` 0.33) supplies the
free-space probe (§3). No hand-rolled civil-date arithmetic.

## 3. On-disk layout

```
$OXIDANT_DATA_DIR/                  # see "Root and precedence" below
  history/
    statements/
      .lock                         # exclusive lock on the journal; a second process fails loudly (§3c)
      seg-000042.jsonl              # append-only records, sealed at 64 MiB
      compacted/
        gen-000007.jsonl            # compaction output (snapshot records), atomically swapped
        gen-000007.done             # swap-intent marker, unlinked last (§4d)
    results/
      <statement-id>.arrow          # Arrow IPC stream; <statement-id> is engine-minted (§4b)
  logs/
    oxidant.log                     # the current file (text, authoritative)
    oxidant-2026-08-23.log          # rolled, awaiting or failing conversion
    oxidant-2026-08-23.parquet      # converted (zstd) — see §6
    oxidant-2026-08-23-14.parquet   # when OXIDANT_LOG_ROLL=hourly
    oxidant-2026-08-23-14.2.parquet # second size-split of that hour — see "Naming" below
    oxidant-2026-W34.parquet        # when OXIDANT_LOG_ROLL=weekly (ISO %G-W%V)
  dumps/
    <dump-id>.parquet               # §6b support bundles, swept like results
```

### Root and precedence

`$OXIDANT_DATA_DIR` is the single root knob. Default: `$XDG_DATA_HOME/oxidant`,
falling back to `~/.local/share/oxidant`; when the process runs as a system service
(euid 0, or `OXIDANT_SYSTEM=1`) the default is `/var/lib/oxidant`. It is **not**
cwd-relative and it is **not** "next to checkpoints" — there is no such convention:
`checkpointLocation` is a per-query, user-supplied **object-store URL** and
`checkpoint.rs` documents at length why treating it as a filesystem path is a bug.

Per-subtree overrides `OXIDANT_HISTORY_DIR`, `OXIDANT_LOG_DIR`, `OXIDANT_RESULT_DIR`
each default to the corresponding subtree of the root. **An explicit override wins
over the root**, and an overridden subtree is still counted against, and pruned by,
the disk budget below.

Every one of these must be a **filesystem path**. A value that parses as an
object-store URL (`s3://`, `gs://`, `az://`, `http(s)://`) is **rejected at boot with
a loud error** rather than silently creating a literal `s3:/bucket/...` directory —
precisely the failure `checkpoint.rs:70–79` warns about. This journal is `std::fs`-only
and node-local by design (§2); `checkpoint.rs` is deliberately *not* its precedent,
because object stores hand it atomicity via `PUT` and give no pattern for durable
filesystem rename (§4d).

### File modes and sensitivity

The journal stores 30 days of **raw SQL**, and the exec log stores every enabled
`tracing` field value — the code already treats both as sensitive (`/api/v1/logs` is
bearer-gated *because* the buffer "names hosts, slots, tables and query text";
`store.rs` redacts credential-shaped env values via `is_secret_env_key`). Moving that
from a 1000-line memory ring to a 30-day on-disk corpus is a real change in exposure,
so:

- Every file the engine creates under the root is mode **0600**; every directory it
  creates is **0700**. Both are set at create time (`OpenOptions::mode`), not chmod'd
  after, so there is no window.
- `OXIDANT_HISTORY_SQL=text|redacted|hash` (default **`text`** — off). `redacted`
  applies the existing `store.rs` credential-shaped-value redaction to the SQL string
  before it is journaled (which does catch the common leaks:
  `OPTIONS(secret '…')`, `s3://key:secret@…`); `hash` journals only a
  `sha256` of the SQL plus its first 120 characters, for operators who want shape
  without text (`sha2` is already in `Cargo.lock` transitively, so promoting it to a
  direct dep of `oxidant-connect` adds nothing new to the tree — Goal 4 holds). The knob is a documented, deliberate trade — under `hash`,
  `GET /api/v1/statements/{id}` after a restart returns the digest, not the query, and
  the doc says so rather than pretending the default is safe everywhere.

### Naming (UTC, collision-free)

All rolled names are computed in **UTC**. This is stated in the runtime contract
because operators read local wall-clock in the log *body*; the *name* is UTC and never
carries an offset. UTC removes the whole DST class: no repeated 01:00 hour, no missing
spring-forward hour, no ambiguous name a prune cannot parse.

```
oxidant-YYYY-MM-DD[.N].{log,parquet}        # daily
oxidant-YYYY-MM-DD-HH[.N].{log,parquet}     # hourly
oxidant-YYYY-Www[.N].{log,parquet}          # weekly, ISO year+week = chrono %G-W%V
```

- `.N` is the **size-split sequence**, present only on the second and later files of
  a period: the first split of 2026-08-23 14:00 UTC is `oxidant-2026-08-23-14.log`,
  the second `oxidant-2026-08-23-14.2.log`, the third `…-14.3.log`. Clock rolls and
  size rolls therefore never produce the same name; the writer picks `N` by scanning
  for the highest existing split of the period at roll time, so a restart mid-period
  does not overwrite.
- Weekly **must** use `%G-W%V` (ISO year + ISO week), not `%Y` + `%W`/`%U`.
  2019-12-30 and 2019-12-31 are ISO **2020**-W01 and must be written
  `oxidant-2020-W01`, together with 2020-01-01..05; the `%Y`+`%W` spelling files the
  December days as `2019-W52` and the January ones as `2020-W01`, splitting one ISO
  week across two names. The other direction is worse: 2021-01-01..03 is ISO
  **2020**-W53, and `%Y`+`%W` writes it `2021-W00`, which the next January silently
  overwrites. A unit test pins both boundaries.
  *(Corrected in PR3.* Rev 2 illustrated this with "2026-12-28..31 is ISO 2027-W01".
  It is not: 2026 begins on a Thursday, so it has 53 ISO weeks and those days are
  2026-W53. The **rule** was right and is unchanged; only the worked example was
  wrong, and `chrono`'s `%G-W%V` produces the correct answer either way — the first
  run of the test caught the doc, not the code.)
- Ordering is **`(period end, split)`, never lexicographic**, and every consumer — the
  prune, `/api/v1/logs/files`, and `AppStateStore::load_event_log` — computes it that
  way. Lexicographic order is chronological only for the plain names of one roll mode:
  a `.N` split is the *newer* generation of its period but sorts *before* the plain
  file, because `'2'` (0x32) < `'l'` (0x6a) / `'j'` (0x6a).

### Retention arithmetic

`OXIDANT_LOG_KEEP_DAYS` (default **30**) is evaluated against the *period* a file
covers, not its name parsed as a day: **a rolled file is deleted only when its whole
period is older than `KEEP_DAYS`.** Weekly therefore rounds *up* — a week file is kept
until its last day falls out of the window, retaining up to 6 extra days. That is the
stated operator contract; the alternative (deleting `W30` at day 30) discards days
that are inside retention.

### Disk guards (hard ceilings — logs must never fill the server)

Everything under the root, **including every overridden subtree and including
`event_log_dir`** (F16 — see §8), lives under one budget and a free-space floor; the
engine deletes the oldest thing it owns before it lets the disk fill:

| Knob | Default | Meaning |
|---|---|---|
| `OXIDANT_DISK_MAX_BYTES` | 8 GiB | total budget for `history/` + `logs/` + `results/` + `dumps/` + `event_log_dir` combined |
| `OXIDANT_DISK_MIN_FREE_BYTES` | 1 GiB | filesystem free-space floor — below it the engine pauses result spill and reports `disk: low_free`; it prunes **nothing** (pruning is driven by `OXIDANT_DISK_MAX_BYTES` alone) |
| `OXIDANT_LOG_MAX_FILE_BYTES` | 256 MiB | the live log rotates **early** at this size, even mid-period (`.N` split) |
| `OXIDANT_LOG_MAX_TOTAL_BYTES` | 2 GiB | `logs/` subtree cap — oldest rolled files deleted first |
| `OXIDANT_EVENT_LOG_MAX_BYTES` | 2 GiB | `event_log_dir` cap (§8) |
| `OXIDANT_LOG_DEDUP` | on | a repeated identical line collapses to `… repeated N times` (§6) |
| `OXIDANT_LOG_PARQUET` | on | `off` keeps rolled files as plain text, ~10× larger, under the same budget (§6) |
| `OXIDANT_LOG_ROLL` | `daily` | `daily\|hourly\|weekly\|off`. **`off` is a PR3 addition** to the three the table listed: the rolling writer is on by default and puts 30 days of every enabled `tracing` field value on disk, and an operator who wants stderr-only logs needed a way to say so that was not "turn statement history off too" |

Free space is read via `sysinfo`'s `Disks` API (`available_space` per mount);
`std` exposes no `statvfs`. The mount for a path is chosen by **longest-prefix match**
of the mount point against the canonicalized path — named explicitly because the naive
"first disk" answer is wrong the moment `OXIDANT_LOG_DIR` points at another volume.
Each distinct mount is floored independently.

**Conversion headroom.** Parquet conversion transiently holds both the text file and
its Parquet output (§6). The sweeper reserves `OXIDANT_LOG_MAX_FILE_BYTES` of headroom
against both the budget and the free-space floor; if the reservation cannot be met the
conversion is **skipped**, the text file is left in place with a `.log` extension, and
it is retried at the next sweep. Conversion never pushes the disk over a guard.

**Prune order when over budget**: oldest rolled logs → oldest dumps → oldest result
files whose statement is already pruned → oldest journal *statements* (§4c —
statement-granular, never raw segment deletion) → oldest live result files. The live
log file is **never** deleted — it rotates instead. If the budget is still exceeded
after everything prunable is gone, `/api/status` reports `disk: over_budget`, the
engine keeps serving, and the log carries one loud line per prune pass naming what was
removed and why. The sweeper runs at roll time, at boot, and every 5 minutes.

**The free-space floor does not prune.** `OXIDANT_DISK_MAX_BYTES` drives the order
above and nothing else does: the engine deletes its own files when its own subtree is
over its own budget. Below `OXIDANT_DISK_MIN_FREE_BYTES` — a shortfall that is very
often a co-tenant's, and one that pruning cannot make satisfiable — the engine instead
**stops writing the large thing**: result spill is paused, `/api/status` reports
`disk: low_free` and `history_writes: degraded`, and no statement record and no result
file is deleted. The journal keeps writing, because its records are small and refusing
them would lose exactly the history this guard exists to protect. The floor is measured
per **mount**, against every managed directory, so a subtree moved to another volume is
floored against that volume.

**Built in PR2** (`crates/oxidant-connect/src/history/disk.rs` + `StatementStore::sweep_disk`),
at boot and on a `OXIDANT_DISK_SWEEP_SECS` timer. **PR3 completed it**: the roll-time
trigger fires from the rolling writer's converter thread (`logging::set_sweep_hook`), and
`event_log_dir` joined the budget — PR2's stated deviation from F16 is closed.

Three notes on what the sweeper measures:

- Steps 1 and 2 were implemented and tested in PR2 even though it wrote nothing under
  `logs/` or `dumps/`. The order *is* the contract, and a sweeper that learns half of it
  later is one that spends a query's rows to save a rolled log.
- **A rolled log is `oxidant-<period>[.N].{log,parquet}` — both extensions.** PR2's
  ownership predicate matched only `.log`, which was harmless while nothing wrote there
  and wrong the moment conversion landed: a rolled log spends most of its life as
  `.parquet`, so step 1 would have skipped every file more than one sweep old and the
  budget would have been paid for out of statement history. The predicate is now the
  *grammar* rather than a prefix and a suffix, so `oxidant-nightly.log` in an operator's
  `/var/log` is no longer claimed either.
- Both prune lists order by **(period, split)**, not by mtime-then-name.
  `oxidant-2026-08-23.2.log` sorts *before* `oxidant-2026-08-23.log` lexicographically
  (`2` < `l`), so the name tiebreak put the newest split at the head of an oldest-first
  list; and a rolled `.log` whose conversion keeps failing is re-touched by every retry
  pass, which made mtime lie about its age.

Two retention passes run **before** the budget on every sweep, because they are the
`logs/` subtree's own contract and `event_log_dir`'s, and must hold whether or not the
global budget is tight: `OXIDANT_LOG_KEEP_DAYS` + `OXIDANT_LOG_MAX_TOTAL_BYTES` over
`logs/`, and `OXIDANT_EVENT_LOG_MAX_BYTES` over `event_log_dir` (§8). A driver far under
8 GiB still may not keep 90 days of logs.

The last step (**oldest live result files**) unlinks the file and journals a snapshot
with the pointer cleared, so a restart answers `410 result_expired` from the fold
rather than from a failed open. The *statement* survives; only its rows go. This is
also the one path that can take a still-running statement's result file — retention
never does, but a last-resort disk guard with nothing else left will, and it keeps the
statement.

### 3c. One process per data dir

Two processes sharing a root would interleave `O_APPEND` writes (atomic only for small
writes — a large `sql` line tears), and both would roll to the same next segment name.
`local-cluster` workers are in-process and fine, but `oxidant worker --port` is a
separate process and the Docker/EC2 topologies routinely start a driver and a worker
from the same working directory.

At boot the engine takes an **exclusive advisory lock** on the *effective statements
directory* — `<history-dir>/statements/.lock`, **not** `$OXIDANT_DATA_DIR/.lock`
(`flock(LOCK_EX|LOCK_NB)` via a small `libc`-free `fcntl` shim — `rustix`/`libc` is
already in the tree through `sysinfo`; if it is not reachable, `O_CREAT|O_EXCL` on a
pid-stamped lockfile with a boot-time staleness check is the fallback). The lockfile
records pid, role (`driver|worker`), port, **and the data dir the holder booted with**.

The lock is on the journal rather than the root because `OXIDANT_HISTORY_DIR` is an
independent knob and an explicit override *wins over the root* (see "Root and
precedence"). Locking the root guards the wrong thing in both directions: two processes
with distinct roots and one `OXIDANT_HISTORY_DIR` would take two different locks, both
succeed, and interleave `O_APPEND` writes into one set of segments — precisely what this
section exists to prevent — while two processes with one root and distinct history dirs
would be refused for no reason. Recording the holder's root is what makes the first case
diagnosable: the error names the data dir the holder booted with, so a collision routed
through `OXIDANT_HISTORY_DIR` reads as one instead of as a phantom.

If the lock is held, the engine **does not** silently share the directory. It fails
with:

```
oxidant: the statement journal (/var/lib/oxidant/history/statements) is locked by pid 4711 (role=driver, port=15002, data dir=/var/lib/oxidant).
         History and logs are per-process. Set OXIDANT_DATA_DIR (or OXIDANT_HISTORY_DIR, which
         moves only the journal) to a distinct path for this process, or set
         OXIDANT_DATA_DIR_PER_PROCESS=1 to use /var/lib/oxidant/<role>-<port>/.
```

When the holder's data dir is *not* this process's, the two are colliding through
`OXIDANT_HISTORY_DIR`, and the advice above cannot help — an explicit history dir wins
over the root, so a distinct `OXIDANT_DATA_DIR` moves nothing. The error says so rather
than sending an operator to a knob that does nothing:

```
oxidant: the statement journal (/srv/history/statements) is locked by pid 4711 (role=driver, port=15002, data dir=/var/lib/oxidant-a).
         This process's data dir (/var/lib/oxidant-b) is not the holder's (/var/lib/oxidant-a), so the two are sharing one
         journal through OXIDANT_HISTORY_DIR. An explicit history dir wins over the root:
         set OXIDANT_HISTORY_DIR to a distinct path for this process. Changing OXIDANT_DATA_DIR,
         including OXIDANT_DATA_DIR_PER_PROCESS=1, will not separate them while it is set.
```

`OXIDANT_DATA_DIR_PER_PROCESS=1` (default off) makes the engine derive
`<root>/<role>-<port>/` automatically instead of failing — the recommended setting for
the container images, which is where co-located driver+worker actually happens. It
separates two processes only when `OXIDANT_HISTORY_DIR` is unset, since the derived root
is what the journal path is then built from.

### Runtime knobs (all runtime-contract documented)

`OXIDANT_HISTORY=on|off` (default on), `OXIDANT_HISTORY_FLUSH_MS` (default 500),
`OXIDANT_HISTORY_ACK_TIMEOUT_MS` (default 2000, §7),
`OXIDANT_HISTORY_MAX_RECORDS` (default 10,000 — see F19/§8 for why this is *not*
today's `MAX_STATEMENTS = 1000`), `OXIDANT_HISTORY_MAX_PER_SESSION` (default 2,000),
`OXIDANT_HISTORY_RETENTION_DAYS` (default **30**), `OXIDANT_HISTORY_SQL`
(default `text`), `OXIDANT_HISTORY_HOT_TTL_SECS` (default 3600, §5b),
`OXIDANT_RESULT_PERSIST=always|on_pressure|never` (default **`on_pressure`** — see
§5 for why this differs from F8's resolution),
`OXIDANT_RESULT_MAX_BYTES` (default 256 MiB per file),
`OXIDANT_RESULT_MEMORY_BUDGET_BYTES` (default **512 MiB**, §5; the spelling
`OXIDANT_RESULT_MEM_BYTES` is accepted as a synonym so a config written against an
earlier draft of this document keeps working),
`OXIDANT_RESULT_DIR` / `OXIDANT_DUMP_DIR` (subtree overrides, §3),
`OXIDANT_DISK_SWEEP_SECS` (default 300 — how often the §3 sweeper runs),
`OXIDANT_LOG_ROLL=daily|hourly|weekly` (default `daily`),
`OXIDANT_LOG_KEEP_DAYS` (default **30**).

## 4. The statement journal

### 4a. Records

One JSON object per line. There are two record shapes and both are **self-contained
enough to be folded alone**:

```json
{"v":1,"kind":"submitted","seq":8812,"id":"stmt-1f3c…","client_op_id":"op-7",
 "session":"0011…","source":"connect","sql":"SELECT …","sql_encoding":"text",
 "status":"pending","submitted_at_ms":1787859731004,"ts":"2026-08-23T18:02:11.004Z"}

{"v":1,"kind":"snapshot","seq":8812,"last_seq":8830,"id":"stmt-1f3c…","client_op_id":"op-7",
 "session":"0011…","source":"connect","sql":"SELECT …","sql_encoding":"text",
 "status":"succeeded","error":null,
 "schema":[["c_customer_sk","Int64"],["n","Int64"]],
 "rows":12,"submitted_at_ms":1787859731004,"duration_ms":143,
 "result":{"file":"stmt-1f3c….arrow","bytes":40912},
 "ts":"2026-08-23T18:02:11.147Z"}
```

**A `snapshot` record carries the complete folded state of one statement** — every
field `StatementSnapshot`/`snapshot_json` needs (`sql`, `source`, `session`, `status`,
`error`, `schema`, `rows`, `submitted_at_ms`, `duration_ms`, `seq`) plus the result
pointer. This is the fix for the review's load-bearing defect: it means a snapshot can
be read in isolation, so compaction (§4d) and pruning (§4c) can drop everything older
without losing state.

Field notes:

- `kind`: `submitted | running | snapshot`. Lifecycle progress is `running`;
  **every terminal transition is written as a `snapshot`**, never as a bare delta.
  There is no separate `finished`/`failed`/`cancelled` kind — the terminal *status*
  lives in the `status` field, which removes the review's redundant
  `"kind":"finished","state":"finished"` pair.
- `status` uses **exactly the live API vocabulary** —
  `pending | running | succeeded | failed | canceled` (American spelling, as
  `StatementStatus::as_str` emits). The journal invents no sixth value; see §4e.
- `seq` is a `u64` assigned by the writer task at append time, monotonic across the
  whole journal and stable for the life of a statement: it is the statement's
  *submit* sequence, which is exactly what `StoreInner.next_seq` means today
  (newest-first `list()`, oldest-first eviction). `last_seq` on a snapshot is the
  writer sequence of the newest event folded into it.
- `source`: `rest | connect` — Connect `ExecutePlan` submits with `connect`, which is
  what unifies the history for issue #134. The record is written **at submit**, so a
  crash mid-statement still leaves a trace with its SQL.
- `sql_encoding`: `text | redacted | hash`, echoing `OXIDANT_HISTORY_SQL` at write
  time, so a journal whose policy changed mid-life is still readable honestly.
- `ts` is RFC-3339 UTC and is human-facing only; **all ordering and age arithmetic
  uses `seq` and `submitted_at_ms`**, never the string.

### 4b. Identity: the id is engine-minted, always

Connect's `operation_id` is client-supplied (`lib.rs:1198–1202` takes
`req.operation_id` verbatim and only falls back to `Uuid::new_v4()` on empty) and
today unvalidated. Two consequences the design must not inherit: a client could set it
to `../../../../home/oxidant/.ssh/authorized_keys` and steer the `results/<id>.arrow`
write; and Spark Connect scopes op ids *per session*, so two sessions reusing `op-1`
would merge into one history entry and overwrite each other's result file.

Therefore:

1. **The statement id is always `stmt-<uuid-v4>`, minted by the engine**, on both the
   REST and Connect paths. It is the only thing that ever reaches a filesystem path,
   and it is `[a-z0-9-]` by construction.
2. The client string is retained as an **alias**, `client_op_id`, after validation:
   `^[A-Za-z0-9._:-]{1,128}$`. A value that fails validation is **not** an error —
   the statement runs, the alias is recorded as `null`, and one WARN line names the
   session. (Rejecting the execution would make a logging concern break queries.)
3. Lookup by alias is `(session, client_op_id) → stmt-id`, an in-memory index rebuilt
   at replay. **The pair is the alias key; `client_op_id` alone is never a key**, so
   cross-session collisions cannot merge. Connect's `Interrupt`/reattach paths keep
   using `operation_id` against the existing in-flight map exactly as today — nothing
   in this design changes gRPC-level identity, only what gets journaled and named.
4. `stmt-<uuid>` is the fold key. `client_op_id` is never a fold key.

**Per-session share of the cap.** A single client could otherwise mint 10,000 op ids
and evict every other tenant's history. `OXIDANT_HISTORY_MAX_PER_SESSION` (default
2,000) bounds any one session's live records; eviction within a session is
oldest-terminal-first, and it runs *before* the global `OXIDANT_HISTORY_MAX_RECORDS`
sweep, so a noisy session evicts itself first.

### 4c. Replay, ordering, and statement-granular pruning

**Replay order is defined by `seq`, not by file order.** The fold applies a record for
`id` only if its `last_seq` (or `seq`, for non-snapshot kinds) is **greater than or
equal to** the sequence already folded for that id. That makes the fold
order-independent and idempotent, which in turn makes double-folding after a crashed
compaction swap (§4d) harmless.

Files are nonetheless read in a defined order so that quarantine and prune are
deterministic: `history/statements/compacted/gen-*.jsonl` first, ascending by the
**numerically parsed** generation, then `history/statements/seg-*.jsonl` ascending by
the **numerically parsed** segment number. Directory iteration order is never trusted.
Because the fold is seq-monotone, a compacted snapshot that is newer than a live event
for the same id wins regardless of which file was read first.

**Non-terminals at replay.** A statement left `pending`/`running` at shutdown replays
as `failed` with `error: "interrupted by restart"`. It does **not** introduce a sixth
status (§4e). Its `cancel: watch::Sender` is correctly not a problem — a replayed
statement is terminal, so no cancel channel is needed.

**Corruption.** A line that does not parse stops replay *of that file* at the first
bad line; the file is renamed `…jsonl.corrupt` (kept, not deleted, and counted against
the budget) and boot continues with the remaining files. History must never be the
reason the engine does not start.

**Bounded replay** (Goal 5): replay reads at most `OXIDANT_HISTORY_MAX_RECORDS`
statements, newest-first by segment, and stops. Older files are left for the sweeper.

**Pruning is statement-granular.** Retention (`RETENTION_DAYS`, `MAX_RECORDS`,
`MAX_PER_SESSION`) selects *statements*, oldest-terminal-first; a non-terminal
statement is never evicted. Removing a statement means: append a `{"kind":"tombstone",
"id":…,"seq":…}` record, unlink its result file (§5), and let compaction physically
drop it. **A raw segment file is deleted only when every statement it names is either
tombstoned or has a snapshot in a compacted generation** — which is exactly the
invariant compaction establishes. This is why the review's "delete the oldest segment"
prune step is gone from §3: a statement whose `submitted` lived in `seg-41` and whose
snapshot lives in `seg-42` must never lose its SQL because `seg-41` aged out.

### 4d. Compaction: seal first, swap atomically, fsync the directory

- **Only sealed segments are compacted.** The writer seals a segment at 64 MiB (or at
  boot, or on demand from the compactor) by closing it and starting `seg-N+1`; the
  compactor never touches the segment the writer holds open. The seal point is the
  synchronization primitive — there is no lock between compactor and writer.
- A pass runs when the superseded ratio in sealed segments passes 50%. It reads the
  sealed inputs, folds them, and emits **one `snapshot` record per surviving
  statement** (F1: not the terminal event — the full folded state), preserving each
  statement's original `seq` and setting `last_seq`.
- **The swap**, in order, with an explicit marker so the two-step is recoverable:
  1. write `compacted/gen-000007.jsonl.tmp`, `fsync` the file;
  2. `rename` to `compacted/gen-000007.jsonl`, **`fsync` the `compacted/` directory**;
  3. write + `fsync` `compacted/gen-000007.done` listing the input segment names,
     `fsync` the directory;
  4. unlink each input segment, `fsync` `statements/`;
  5. unlink the `.done` marker, `fsync` `compacted/`.

  Boot recovery: a `.tmp` is deleted; a `.done` present means step 4 may be
  incomplete, so boot re-runs the unlinks and then removes the marker. A crash
  anywhere leaves at worst a double-fold, which the seq-monotone fold (§4c) absorbs.
- **Parent-directory fsync is mandatory for every rename in this design** — segment
  seal, compaction swap, result file publish, log roll, Parquet publish. On ext4/xfs a
  `rename()` is not durable until the containing directory is fsynced. There is **no
  existing in-tree pattern to inherit**: `checkpoint.rs` gets atomicity from
  object-store `PUT` and says so. This design is the first filesystem-durability path
  in the tree and spells the rule out rather than assuming it.

### 4e. Journal ↔ API vocabulary

The journal uses the API's words verbatim; there is no translation table to get wrong.

| Journal | Live API (`StatementStatus`) | Note |
|---|---|---|
| `kind:"submitted"`, `status:"pending"` | `pending` | the record kind names the *event*, `status` names the *state* |
| `kind:"running"`, `status:"running"` | `running` | |
| `kind:"snapshot"`, `status:"succeeded"` | `succeeded` | |
| `kind:"snapshot"`, `status:"failed"` | `failed` | `error` carries the text |
| `kind:"snapshot"`, `status:"canceled"` | `canceled` | American spelling, matching `as_str()` |
| *(replay of a non-terminal)* | `failed` + `error:"interrupted by restart"` | **no new status value** |

`interrupted` is deliberately **not** a status. Adding a sixth value would break every
client switching on the documented five; `failed` with an explicit error string needs
no contract change and reads identically to a human.

## 5. Result retention

**Built in PR2** (`crates/oxidant-connect/src/history/results.rs`, `rest.rs`). What
follows is what the code does; the two places it diverges from the design as written
are called out inline.

- **Default is `on_pressure`.** *Deviation from the review's F8 resolution, which set
  the default to `always`.* Rows stay in memory until the in-memory budget would be
  exceeded; nothing is written on a quiet driver. The trade is stated plainly rather
  than hidden: under the default, a clean restart persists only what pressure already
  pushed to disk, and an operator who wants Goal 2 unconditionally sets
  `OXIDANT_RESULT_PERSIST=always` — which is implemented, tested, and one env var
  away. The default was chosen so that turning history on does not, by itself, start
  writing every query's rows to an operator's disk.
- `on_pressure` is backed by a real trigger: `OXIDANT_RESULT_MEMORY_BUDGET_BYTES`
  (default **512 MiB**, also accepted under §3's original spelling
  `OXIDANT_RESULT_MEM_BYTES`) is an **in-memory byte budget** across retained result
  batches, tracked as batches are attached and released. Today's store evicted by TTL
  and count only and had no byte accounting at all; PR2 adds it. A result spills when
  admitting a new one would exceed the budget, **oldest-terminal-first**, and the
  victim's rows are released only once its file is durable.
- Under `never` nothing is written **and nothing is released**: a byte budget with no
  disk behind it would be silent data loss the old store never had, so `never` leaves
  memory bounded by the count cap and the hot TTL exactly as before.
- **Spill never runs under the store mutex.** Writing 256 MiB of Arrow IPC while
  holding the `std::sync::Mutex` that every submit/list/status/result call takes is the
  exact opposite of "a query never waits on history". `finish()` *plans* the spill under
  the lock, doing no I/O, and a dedicated writer thread — same shape as the journal's —
  encodes, writes `<id>.arrow.tmp`, fsyncs, renames, fsyncs `results/`, and only then
  appends the snapshot record carrying the `result` pointer. A pointer replay reads has
  therefore always named a file that reached the disk.
- Results larger than `OXIDANT_RESULT_MAX_BYTES` are refused; the statement's snapshot
  records `result_too_large` in place of the `result` pointer, and
  `GET /api/v1/statements/{id}` surfaces it as `"resultStatus": "result_too_large"` so
  the eventual `410` does not read as "it merely aged out". The cap is enforced **on the
  encoding, while writing**, not on an in-memory estimate: `get_array_memory_size`
  over-counts shared Arrow buffers, so refusing on the estimate would refuse results that
  encode well under the cap. The live/CSV path is the answer past the budget.
- A **zero-batch** result has no file: an Arrow IPC stream cannot be written without a
  schema, and a succeeded statement that produced no batches has none. It answers
  `200 {"rows":[]}` — before **and** after a restart. The terminal snapshot that was being
  written anyway records `result_empty` in place of the pointer, and
  `GET /api/v1/statements/{id}` surfaces it as `"resultStatus": "result_empty"`. Answering
  `410` after a restart would have made a *correct* empty answer — DDL, and in DataFusion
  plenty of ordinary empty result sets — indistinguishable from data loss, at the cost of
  no extra write and no extra byte on disk.
- **Result GC is tied to the journal, and the journal is the authority.** Pruning a
  statement (§4c) unlinks its result file in the same sweep, before the tombstone is
  considered complete. Boot reconciles `results/` against the folded id set and
  deletes every unreferenced file — which closes the crash window between "tombstone
  appended" and "file unlinked". A result file therefore outlives its statement's
  journal record by at most one retention sweep, and never across a restart. The
  reconcile is run against the union of **both tiers**, never the folded set alone: a
  hot statement has no snapshot on disk yet, and a running one has nothing past its
  `submitted` record.
- A `snapshot` record's **absent** `result` now *clears* a pointer rather than leaving
  it unchanged, on both sides of the fold. §4a already says a snapshot carries the
  complete folded state; making the fold honour that is what lets §3's last prune step
  say "this result is gone" instead of leaving a pointer to a file that is not there.
- `/api/v1/statements/{id}/result` (`?format=json|csv`) reads memory → falls back to
  the spilled file → answers **`410 result_expired`** when both are gone. The file is
  decoded on a blocking thread, so a 256 MiB read-back never sits on a tokio worker.

  **This is a contract change, announced, not disclaimed.** Today `/result` answers
  `404 unknown statement id`, `409` for a non-succeeded statement, and `400` for a bad
  format; `410` and the string `result_expired` exist nowhere in the tree. The new
  code is added deliberately: `404` keeps meaning "no such id", and `410` means "the
  statement is known and succeeded, but its rows are gone". Clients that treat any
  non-2xx as failure are unaffected; the runtime contract and the API changelog both
  gain an entry.

## 5b. The two-tier read model

The single most important consequence of persistence: **`STATEMENT_TTL` (1 h) and
`evict_expired()` on every insert would delete the replayed 30 days on the first new
submit.** Replay that no endpoint can observe is not replay. So the store becomes two
tiers:

| Tier | Contents | Bound | Eviction |
|---|---|---|---|
| **Hot** | live + recently-terminal statements with their in-memory batches and `cancel` channels | `OXIDANT_HISTORY_HOT_TTL_SECS` (3600, today's TTL) and `MAX_RECORDS` | as today, but see the age fix below |
| **History** | folded snapshots from the journal — no batches, no cancel channel | `OXIDANT_HISTORY_MAX_RECORDS` (10,000) index in memory; SQL/schema read from the journal on demand | `RETENTION_DAYS` / `MAX_RECORDS` / `MAX_PER_SESSION`, by the 5-minute sweeper only |

- **Replay populates the history tier, never the hot tier.** `evict_expired` runs only
  over the hot tier, so the first post-boot insert cannot drop replayed state. This is
  the single line the review's F5 turns on.
- `GET /api/v1/statements/{id}` reads hot → falls through to history.
  `GET /api/v1/statements` merges both, newest-first by `seq`, and reports
  `"tier":"hot"|"history"` per row so the UI can tell a cancellable statement from an
  archival one. `POST …/cancel` against a history-tier statement answers `409`, as it
  does today for a terminal statement.
- **Age is wall-clock, not `Instant`.** `evict_expired` today computes
  `Instant::now().duration_since(s.submitted)`; `Instant` is monotonic with no epoch,
  so it cannot be reconstructed from a journaled timestamp, and synthesizing
  `Instant::now() - age` saturates or panics for ages beyond process uptime — the
  common case for a 30-day journal. `Statement` already carries `submitted_at_ms: i64`
  (it is on `StatementSnapshot` too), so eviction switches to `submitted_at_ms`, and
  `Instant` is retained **only** for live `duration_ms` on statements this process
  actually ran. Replayed statements have no `Instant` and need none: their
  `duration_ms` came off the journal.

## 6. Rolling exec logs, compressed

*Built in PR3*: `crates/oxidant-connect/src/logging/` — `naming` (UTC names, the
`?file=` grammar), `line` (one event's three forms), `writer` (the live file, both roll
triggers, dedup, the converter thread) and `columnar` (text → zstd Parquet). Process-level
init is `oxidant_connect::logging::init(role, port)`.

- **The rolling writer is a `tracing` layer in its own right**, not a re-serializer of
  `LogBuffer` strings. `format_event` emits `[LEVEL] target - fields` with **no
  timestamp and no span**; converting that to a columnar log would yield a Parquet file
  with no usable time column, and §6b's time-range filters would have nothing to filter
  on. The layer therefore taps `tracing` directly and writes structured fields.
  `format_event` additionally gains an RFC-3339 UTC timestamp prefix so the *text*
  file, the live tail, and the Parquet all agree on time.
- **Text is authoritative; Parquet is a derived form.** The live file `oxidant.log` is
  line-oriented text, appended and periodically flushed. This preserves the crash
  honesty of §4 — a torn tail loses the last lines, not the file.
- **Two roll triggers, whichever first**: the UTC clock boundary (`daily` default,
  `hourly`/`weekly` via `OXIDANT_LOG_ROLL`) or the size cap
  (`OXIDANT_LOG_MAX_FILE_BYTES`), which produces a `.N` split (§3 Naming) — a chatty
  hour rotates early instead of growing without bound.
- **Conversion happens after close, never during.** Exactly:
  1. close `oxidant.log`, `fsync` it, `rename` to `oxidant-<period>[.N].log`,
     `fsync` `logs/`, open a fresh `oxidant.log`;
  2. *then*, on the sweeper's task, convert `oxidant-<period>[.N].log` →
     `…​.parquet.tmp` (schema `(ts, level, target, message, fields_json)`, zstd, using
     the Arrow/Parquet stack the engine already ships), `fsync`, `rename` to
     `.parquet`, `fsync` `logs/`, read the footer back, and only then unlink the text
     file and `fsync` `logs/` again.

  **A crash between the roll and the convert leaves the rolled `.log` text file on
  disk; it is converted on the next boot's sweep.** Parquet's footer-at-the-end means
  a half-written Parquet is not partially readable at all — which is why the text file
  is never removed before the footer reads back, and why a `.parquet.tmp` found at boot
  is simply deleted and the conversion redone. A conversion that fails twice leaves the
  `.log` in place permanently and logs one loud line; `?file=` serves it as text.
- **The cost, stated:** once converted, an operator can no longer `tail`/`grep`
  yesterday's log with shell tools. That is a real loss and it is the price of ~10×
  compression plus predicate-pushdown browsing (§6b). Two outs, both documented:
  `OXIDANT_LOG_PARQUET=off` keeps rolled files as plain text (subject to the same
  budget, and they will be roughly 10× larger), and `POST /api/v1/logs/dump` (§6b)
  produces a downloadable slice.
- **Repeated-line suppression** (`OXIDANT_LOG_DEDUP`): an identical consecutive line is
  held and counted, and its `… repeated N times` summary is flushed on **any** of:
  a different line arriving, a 5 s timer (so a process that repeats one line and then
  goes quiet still writes the count promptly and the file's last entry is never stale),
  a roll, or shutdown. A hot error loop cannot fill the disk between two sweeps.
  **Divergence, stated:** dedup applies to the *file*; the in-memory `LogBuffer` live
  tail is not deduped, so the same window can read differently. **The file is
  authoritative**, and `/api/v1/logs?file=current` says so via
  `"dedup":true` in its response envelope; the SSE tail marks itself
  `"dedup":false`.
- Retention: `OXIDANT_LOG_KEEP_DAYS` (default **30**, period-based — §3) plus the hard
  guards in §3. The size budget can prune a file before its 30 days are up, and says so
  in the log when it does.

### `?file=` grammar, validation, and authz

```
file := "current"
      | YYYY "-" MM "-" DD          [ "." N ]        # daily
      | YYYY "-" MM "-" DD "-" HH   [ "." N ]        # hourly
      | YYYY "-W" ww                [ "." N ]        # weekly, ISO
YYYY := 4DIGIT   MM,DD,HH,ww := 2DIGIT   N := 1*3DIGIT, 2..999
```

- The parameter is **parsed into a typed `LogPeriod` enum** and the filename is
  *reconstructed* from that value. It is never string-joined into a path, never
  contains an extension, and a value that does not match the grammar answers
  `400 invalid file`. `..`, `/`, and every other traversal shape fail the grammar by
  construction — the same discipline as §4b.
- Extension is chosen by the server: `.parquet` if present, else `.log`, else
  `404`, matching §6's conversion states. Callers never name an extension.
- **Authz is the same gate as today, restated because it now matters more:**
  `/api/v1/logs` is wrapped by `deny_unless_authorized(status_token, …)` and 404s when
  `OXIDANT_STATUS_TOKEN` is unset. `?file=` inherits it unchanged — every new endpoint
  in §6b does too. The endpoint now exposes up to 30 days rather than 1000 lines, so
  the runtime contract repeats the gate in the operator-facing text rather than leaving
  a reader to assume it.
- (The rev-1 grammar's `[‑HH]` used U+2011, a non-breaking hyphen. It is `-`.)

### 6c. Workers get the same writer

`init_logging()` is today the only `tracing_subscriber` init in the tree and it is
called from `rest::router`, which is built only in the Connect server bootstrap. A
standalone `oxidant worker` therefore installs no subscriber and would get no durable
log — and worker OOMs are exactly what operators dig for.

So: the subscriber init (`LogBuffer` + rolling writer + fmt layer) is **hoisted out of
the REST router into a process-level `oxidant_connect::logging::init(role, port)`** that
both the Connect server bootstrap and `run_worker` call. (*Deviation from this section's
`init(role)`.* `OXIDANT_DATA_DIR_PER_PROCESS=1` derives `<root>/<role>-<port>/`, and
without the port the logging init would derive `<root>/driver/` while the journal derived
`<root>/driver-15002/` — one process's logs and its own statement history in two different
trees. `rest::router` still calls it with `("driver", 0)` as an idempotent fallback for an
embedded caller that builds the router directly.) Every node writes its own
`logs/` under its own root (§3c). **Collection stays per-node** — the driver does not
ingest worker logs; it federates reads over them at query time (§6b), which is the
same statement from the other side. Statement history remains driver-scoped: workers
run no statements.

## 6b. Browsing logs on the driver (and workers, without moving them)

Logs stay **where they are written**. Browsing is a query-time read through
REST/gRPC interfaces — the driver never ingests worker logs into its own store;
the one exception is an explicit diagnostic dump (below). This is the read-side of
§6c: same writer everywhere, collection per node, federation at read time.

**Driver log browser API** (token-guarded by `deny_unless_authorized`, exactly as
`/api/v1/logs` is today):

```
GET /api/v1/logs/files                     → [{file, rolled, format, size_bytes, first_ts, last_ts}]
GET /api/v1/logs?file=…&level=warn&target=oxidant_execution&q=pool&
     from=…&to=…&limit=500&before=<cursor>   → {lines: [...], next_before, dedup}
GET /api/v1/logs/tail?file=current&level=… (SSE)               → live follow
```

- Filters compose: level (≥), target prefix, free-text `q`, time range; results
  stream newest-first with a cursor so the UI pages backward without reloading
  the whole file. `tail` is the SSE follow mode over the live file.
- `file` is the typed grammar above; `from`/`to` are RFC-3339 and are matched against
  the `ts` column — which exists because §6 makes the writer emit one.
- Rolled files answer the same query shape — the Parquet reader evaluates the
  level/target/time predicates as column filters, so browsing a compressed day
  does not decompress the whole day. A rolled file still in `.log` form (conversion
  pending or failed) is scanned as text and marked `format:"text"`.
- `GET /api/v1/logs/files` works for every file the sweeper has not pruned —
  the visible history is always honestly what exists.

**Worker logs through the driver** (federation, not shipping):

```
GET /api/v1/logs/workers                   → [{worker_id, address, reachable}]
GET /api/v1/logs?worker=<id>&…same query…  → proxied from that worker's own /api/v1/logs
```

Every worker runs the identical logs API over its own files (§6c). The driver
proxies the query (with the same filters, and its own status token) at read time and
labels the rows with their worker — the Observability screen's worker picker is a
dropdown of live workers plus "driver". A worker that does not answer is listed
`reachable: false` and its rows are absent with the reason — never silently skipped.
No worker log bytes touch the driver's disk on this path.

**Diagnostic dump (the only time logs move):**

```
POST /api/v1/logs/dump {worker: <id>|"all", from, to}  → 202 {dump_id}
GET  /api/v1/logs/dump/{dump_id}                       → parquet bundle download
```

An explicit, token-guarded, audited action for support bundles: the worker(s)
stream the requested slice (already-Parquet rolled files ship as-is; the live
file converts on the fly), the driver assembles one download into `dumps/`. Bounded
by `OXIDANT_LOG_DUMP_MAX_BYTES` (default 1 GiB) and by the §3 budget — a dump that
would breach either is refused with `507`, not silently truncated. The bundle expires
after 24 h and is swept like results.

**Observability screen**: the log section gains the file picker (current +
rolled dates), level/target/text filters, time range, tail-follow toggle, and
the worker dropdown — all over the endpoints above, same token flow as today.

## 7. Failure semantics (the honesty section)

**The guarantee, in one sentence:** *a statement's terminal state is durable before its
client is told the statement finished; intermediate lifecycle events may be lost, up to
one flush interval, on a crash.*

That is a deliberate choice among the three the review identified as mutually
exclusive. Concretely:

- **Execution never waits on history.** The query future, the result batches, and the
  in-memory store are never blocked by the writer. A stalled disk cannot stall a
  statement; it can only delay a *response* and then degrade it.
- **The response — not the query — awaits the terminal fsync.** When a statement
  reaches a terminal state, `StatementStore::finish` hands the snapshot to the writer
  with a oneshot ack and *then* does everything it does today (updates memory, calls
  `notify_waiters()`). The response path (`?wait=true`, the Connect terminal message)
  awaits that ack for at most `OXIDANT_HISTORY_ACK_TIMEOUT_MS` (default 2000 ms). On
  ack, the client's answer is durable. On timeout, the response goes out anyway
  carrying `"history":"degraded"` in its envelope, `/api/status` flips
  `history_writes: degraded`, and the promise is explicitly downgraded to best-effort
  for that statement. The wait is bounded on a stalled disk by construction.
- **A refused spill is retried, never stranded.** The spill queue is bounded (256 jobs).
  A job it has no room for is counted in `history_dropped_events` and handed straight
  back to the statement store, which clears the statement's `spilling` mark so the very
  next budget pass can select it again. The rows never left memory and never left the
  budget's accounting; the retry is the next terminal statement, by which time the queue
  has had a chance to drain. Same for a spill the disk refuses outright. Leaving the mark
  set on a job nobody took pinned the rows for the whole hot TTL and removed the
  statement from the budget's reach.
- **Backpressure, precisely.** The writer channel is bounded (4096 records). When it
  is full: `running` records are **dropped**, counted, and surfaced as
  `history_dropped_events` on `/api/status` — they are progress chatter and losing
  them costs nothing the fold needs. `submitted` and `snapshot` records are
  **never dropped and never coalesced**; a full channel makes their *sender* wait —
  the response task, under the ack timeout above, or the sweeper's own task for
  background writes. Nothing that waits is on the execution path.
- **Crash window**: ≤ one `OXIDANT_HISTORY_FLUSH_MS` (500 ms) of `running` records.
  Acked terminal states are durable. Unacked terminal states are the degraded case,
  and the client was told so.
- **ENOSPC / EIO**: appends start failing → `/api/status` reports
  `history_writes: degraded` (and `disk: over_budget` if §3's sweeper has run out of
  things to prune); the engine keeps executing. Recovered disk flips it back without a
  restart.
- **Degraded is per subsystem.** There are three — the journal, the result spill writer,
  and the disk sweep — and each one's flag is sticky until a success **of its own**
  clears it. `history_writes` is the aggregate (`degraded` while any is), `result_writes`
  is the spill writer alone, and `disk` is the sweep's. Reporting a spill failure through
  the journal's flag made it invisible: the journal clears its own on every successful
  append, so a permanently failing `OXIDANT_RESULT_DIR` read `ok` again the microsecond
  the next statement was submitted. `result_write_failures` counts the spills the disk
  refused; `history_dropped_events` counts the journal records *and* spill jobs that
  never reached a writer at all.
- **Corruption**: quarantine-and-continue (§4c). Boot is never blocked by history.

## 8. Compatibility

- Default **on** with the caps above. **`OXIDANT_HISTORY=off` restores today's
  behaviour exactly** — and that now explicitly includes the caps: with `off`, the
  store reverts to the in-memory-only path with `MAX_STATEMENTS = 1000` and
  `STATEMENT_TTL = 3600 s`, no journal, no replay, no result files, no history tier.
  The new knob is named `OXIDANT_HISTORY_MAX_RECORDS` rather than
  `…_MAX_STATEMENTS`, both to avoid colliding with the existing `MAX_STATEMENTS`
  const and to signal that it bounds *journal records folded into the history tier*,
  which is a different population from today's live map.
- **Divergence, documented:** with history **on**, the default is 10,000 records
  against today's 1,000. This is intentional — 1,000 statements is a few hours of a
  busy driver and would make a 30-day retention meaningless — but it *is* a behaviour
  change in what `GET /api/v1/statements` can page through, and it is announced in the
  runtime contract, not slipped in. Operators who want the old volume set
  `OXIDANT_HISTORY_MAX_RECORDS=1000`.
- **`410 result_expired`** is a new status code and a new error string (§5), announced
  in the API changelog.
- **`event_log_dir` comes under the budget** (F16). It is the Spark-history-server
  compatibility surface and the journal stays deliberately separate so each can evolve
  on its own contract — but `AppStateStore::emit` appends every execution event to a
  single `events.jsonl` that is never rolled and never pruned, and `load_event_log`
  reads the whole file back with `fs::read_to_string`. That is the one existing path
  that genuinely fills a server, so a section titled "logs must never fill the server"
  cannot exempt it. It gets its **own** knob, `OXIDANT_EVENT_LOG_MAX_BYTES` (default
  2 GiB), because operators point it at a Spark-history-server path that other tools
  read: when the cap is exceeded the engine rolls `events.jsonl` to
  `events-<UTC-period>.jsonl` (same naming rules as §3) and prunes oldest-first,
  rather than deleting the live file. Setting it to `0` restores today's unbounded
  behaviour, and the runtime contract says what that costs.

  **Two deviations, both found by the test and both argued here** *(PR3)*:

  1. **The roll fires at half the cap, not at the cap.** Rolling only once the live file
     has reached the whole cap makes the very first prune pass delete the file it just
     created: the directory then oscillates between one full file and none, and an
     operator loses every event at each roll. Half the cap keeps the ceiling exactly and
     lets a generation survive its own roll.
  2. **The newest rolled generation is never pruned**, so the ceiling may be exceeded by
     at most one generation. The sweep runs every five minutes and `emit` does not stop
     between passes, so one generation can be larger than the whole cap; taking it would
     end every roll with an empty directory. This is the same instinct as §3's "the live
     log file is never deleted — it rotates instead", one file further along.

  The split allocator is **highest-existing + 1**, never "the first free number". Splits
  are pruned out from under it, so first-free hands out `1` again after `.1` has gone —
  and the file just rolled then sorts as the oldest generation of its period and the very
  next prune takes it, keeping the stale `.2`.

  `AppStateStore::load_event_log` reads the rolled generations too, ordered by
  **`(period end, split)`** — the same key the prune uses — and the live file last. A
  history server that read only `events.jsonl` would report a cluster's history as
  starting at the last roll, which is precisely the data loss the roll exists to avoid.

  The order is a *correctness* property, not a presentation one. The fold is
  last-write-wins and `JobStarted` overwrites the whole job (status `Running`, no
  completion time, no error), so replaying a newer generation before an older one brings
  a finished job back as running. A `sort()` over the names does exactly that: the `.2`
  split holding a `JobFinished` sorts before the plain file holding its `JobStarted`.

## 9. Test plan

Journal and fold:

- crash-during-append → replay loses ≤ one flush interval of `running` records, acked
  terminal states survive, and non-terminals replay as `failed` /
  `"interrupted by restart"` — **never** as a sixth status value;
- corrupt tail → quarantined as `.corrupt`, boot succeeds, earlier statements intact,
  other segments still folded;
- **compaction preserves the full folded state per statement** — a compacted journal
  round-trips `sql`, `source`, `session`, `schema`, `error`, `submitted_at_ms`,
  `duration_ms` and `seq` byte-for-byte into `snapshot_json`, and a statement whose
  `submitted` and terminal records were in *different* input segments is intact
  afterwards;
- **segment-granular deletion cannot lose a statement**: delete the oldest segment
  after compaction and assert every retained statement still answers with its SQL;
  assert the invariant directly (no segment is unlinked while it names a statement
  without a snapshot in a newer generation);
- fold is order-independent: shuffle the file read order and the `seq`-monotone fold
  produces an identical state map; feeding the same compacted generation twice
  (simulated crashed swap) changes nothing;
- swap recovery: kill between each of the five swap steps; boot converges to exactly
  one copy of the data in every case;
- replay is bounded at `OXIDANT_HISTORY_MAX_RECORDS` and boot time stays sub-second
  for a 30-day/10,000-record journal.

Identity and tiers:

- a Connect-submitted statement replays with `source: "connect"` (the #134 pin) and
  with its `client_op_id` alias intact;
- `operation_id = "../../etc/passwd"` writes nothing outside `results/`: the id used
  on disk is `stmt-<uuid>`, the alias is recorded `null`, and one WARN is emitted;
- two sessions using `op-1` produce two distinct statements, two distinct result
  files, and two distinct history entries;
- one session cannot evict another's history past `OXIDANT_HISTORY_MAX_PER_SESSION`;
- **replay survives the first new submit** — replay 10,000 statements, submit one, and
  assert `GET /api/v1/statements` still lists the replayed ones (the F5 regression
  test); assert `evict_expired` touched only the hot tier;
- eviction age uses `submitted_at_ms`: a statement journaled 40 days ago folds and ages
  correctly with no `Instant` reconstruction anywhere in the path.

Results *(shipped in PR2, in `rest.rs`'s test module unless noted)*:

- `always`: result spill → process restart → `/result` **and** `/result?format=csv`
  answer byte-for-byte identically to the pre-restart answer;
- oversized result → `result_too_large` on the statement, no `.arrow` and no `.tmp`
  left behind, the rows kept in memory because they are now the only copy, and
  `410 result_expired` after the restart;
- `on_pressure` spills exactly when the in-memory budget would be exceeded, picks the
  **oldest terminal** result, and frees its memory — the newest stays servable from
  memory;
- `never` writes nothing and releases nothing;
- a **refused** result stays in the byte budget's accounting and leaves its candidate
  set, so the store converges to the budget plus that result rather than re-selecting
  and re-declining it forever;
- a spill the queue **drops** and a spill the disk **refuses** both put the statement
  back into the budget's candidate set, and the next pass writes it;
- a spill that lands after its statement was evicted publishes nothing: no file, no
  pointer, and no resurrection across a seal-and-compact that drops the tombstone;
- a **zero-batch** result answers `200 {"rows": []}` before and after a restart, and
  says `resultStatus: "result_empty"` both times;
- a wide schema — dictionary, nullable struct with nulls, timestamp with timezone —
  round-trips through the file and the restart verbatim;
- pruning a statement unlinks its result in the same sweep; boot deletes `results/`
  files referenced by no folded id; and a **running** statement's result survives every
  retention path there is.

Logs *(shipped in PR3; the writer's own tests live beside it in
`crates/oxidant-connect/src/logging/`, the retention and `event_log_dir` tests in
`history/disk.rs` + `rest.rs`, and the two-entry-point test in
`crates/oxidant-cli/tests/cli_rolling_logs.rs`)*:

- log roll across a fake UTC clock at daily/hourly/weekly boundaries, 30-day
  period-based prune;
- the size roll fires mid-period and produces `.2`, `.3` splits that never collide
  with the clock-rolled name, across a restart mid-period;
- ISO week naming: 2026-12-28..31 write `oxidant-2027-W01`, not `oxidant-2026-W01`;
- rolled text → Parquet round-trip returns the same rows with a usable `ts` column; a
  crash between roll and convert leaves the `.log` and the next boot converts it; a
  failed conversion keeps the text file and `?file=` serves it as text;
- conversion is skipped, not attempted, when headroom would breach a §3 guard;
- the dedup guard collapses a hot loop into `… repeated N times` and flushes the count
  on the 5 s timer with no further input;
- `?file=` grammar: every valid form resolves, `..`/`/`/extensions/absolute paths all
  answer `400`, and an unset `OXIDANT_STATUS_TOKEN` 404s the whole endpoint;
- `run_worker` alone (no REST router) writes a rolling log *(shipped: a subprocess test,
  because the bug it guards is a wiring bug and only the real binary can say whether the
  init is reachable from both entry points)*, and the driver federates a query over it; an
  unreachable worker is reported `reachable:false`, not skipped *(PR4)*.

Guards and degradation:

- the disk-budget sweeper prunes in the documented order, never touches the live
  file, only ever unlinks files it can recognise as its own (`oxidant-<period>[.N].log`
  *and* `.parquet`, `dump-*.parquet` / `oxidant-*.parquet`, `stmt-*.arrow`,
  `events-<period>[.N].jsonl`), measures the tree exactly twice per pass however many
  statements it prunes, and reports `over_budget` only after everything prunable is gone
  *(shipped: PR2, with the `event_log_dir` half and the rolled-`.parquet` half in PR3 —
  see §3)*;
- a free-space shortfall the engine did not cause deletes **nothing**: with the engine
  far under `OXIDANT_DISK_MAX_BYTES` and the volume under
  `OXIDANT_DISK_MIN_FREE_BYTES`, one sweep prunes no statement, no result, no log and
  no dump, pauses spill, and reports `disk: low_free` + `history_writes: degraded`. The
  same store with its *own* budget exceeded still prunes, in the documented order, and
  reports `disk: over_budget`. The floor is checked against every managed directory's
  mount, not just the root's;
- `/api/status` reports `history_writes: degraded` under a failing-writer shim and
  flips back to `ok` on the next successful append, with no restart; a failing *spill*
  writer is reported by `result_writes` / `result_write_failures` and is **not** cleared
  by a healthy journal append; with `OXIDANT_HISTORY=off` all six durability fields are
  absent entirely. Asserted end to end, over the real `GET /api/status` route with its
  bearer token, not through a test seam *(shipped)*;
- two processes on one root: the second fails with the lock error; with
  `OXIDANT_DATA_DIR_PER_PROCESS=1` it starts in its own subdir;
- an object-store URL in `OXIDANT_DATA_DIR` is rejected at boot;
- files are 0600 and directories 0700 at creation; `OXIDANT_HISTORY_SQL=redacted`
  keeps `OPTIONS(secret '…')` out of the journal; `=hash` stores a digest and the API
  says so;
- degraded mode under a failing-writer shim: statements execute, the terminal ack
  times out, the response carries `history: degraded`, status reports `degraded`, and
  recovery is automatic;
- `OXIDANT_HISTORY=off` reproduces today's behaviour byte-for-byte on the existing
  REST test suite, including the 1000-statement cap and the 1 h TTL.

## 10. Rollout

1. **PR1** — *shipped* — the journal (snapshot records, seq, engine-minted ids, lock, fsync
   discipline) + replay + the two-tier read model + Connect unification
   (closes #134's durable half). Lives in `crates/oxidant-connect/src/history/`; the two-tier
   store is `rest.rs`. Two deviations, both deliberate: `410 result_expired` ships here rather
   than in PR2, because a history-tier statement whose rows are gone must not answer `404` or an
   empty result set in the meantime; and `/api/status` does not yet carry `history_writes` /
   `history_dropped_events` — the per-response `"history":"degraded"` envelope does, and the
   status fields land with PR2's disk guards (they did).
2. **PR2** — *shipped* — result spill (writer thread, byte budget) + disk fallback in
   `/result` and `/result?format=csv` + result GC tied to the journal + §3's disk guards
   and the `/api/status` counters PR1 deferred (`history_writes`,
   `history_dropped_events`, `results_on_disk_bytes`, `disk`). Three deviations, each
   argued where it lands: the persist default is `on_pressure` rather than F8's
   `always` (§5), the in-memory budget is `OXIDANT_RESULT_MEMORY_BUDGET_BYTES` at
   512 MiB rather than `OXIDANT_RESULT_MEM_BYTES` at 1 GiB — the old spelling is still
   accepted (§3, §5) — and `event_log_dir` joins the disk budget in PR3 rather than
   here, because F16's mechanism for it is a roll and the rolling writer is PR3 (§3).
3. **PR3** — *built* — process-level logging init (driver *and* worker), the rolling
   writer as a `tracing` layer in its own right with RFC-3339 UTC timestamps, UTC naming
   with `.N` size splits, Parquet-on-roll, dedup, the roll-time disk sweep plus `logs/`
   retention, `event_log_dir` under the budget by rolling (closing PR2's stated F16
   deviation), and `?file=` on `/api/v1/logs`. Lives in
   `crates/oxidant-connect/src/logging/`. **Five deviations, each argued where it lands:**
   `logging::init` takes `(role, port)` rather than §6c's `(role)`, so a process's logs
   and its journal derive the same `<role>-<port>` root (§6c); `OXIDANT_LOG_ROLL` gains an
   `off` value beside `daily|hourly|weekly`, so an operator can keep durable statement
   history with stderr-only logs (§3); the event log rolls at **half**
   `OXIDANT_EVENT_LOG_MAX_BYTES` rather than at the cap, and its **newest rolled
   generation is never pruned**, because rolling at the cap makes the first prune delete
   what it just created and the directory oscillates between one full file and none (§8);
   and §3's worked ISO-week example was arithmetically wrong — 2026-12-28..31 is 2026-W53,
   not 2027-W01 — so the example is corrected and the test pins 2019-12-30 → 2020-W01 and
   2021-01-01 → 2020-W53 instead (§3). Two things the tests caught in the *code*: the
   converter leaked a `.parquet.tmp` when the source failed partway through a read (macOS
   opens a directory happily and fails at the first read), and both prune lists ordered
   `.2` ahead of the plain name because `2` < `l` lexicographically.
4. **PR4** — the driver log browser (filters/cursor/tail) + worker federation +
   the diagnostic dump, with the Observability screen's log UI.

## 11. Review resolutions (F1–F21)

| # | Finding | Resolution |
|---|---|---|
| **F1** | Compaction destroys `sql`/`source`/`session` | **Design change.** §4a introduces the self-contained `snapshot` record carrying every field the fold and `snapshot_json` need; §4d makes compaction emit one per surviving statement instead of the terminal event. §9's bullet is rewritten from "preserves exactly the terminal states" to "preserves the full folded state per statement", with a cross-segment case. |
| **F2** | Segment-granular pruning breaks the fold | **Design change.** §4c makes pruning statement-granular (tombstone + result unlink + compaction), and states the invariant that a segment is unlinked only when every statement it names is tombstoned or snapshotted in a newer generation. The "oldest journal segments" step is gone from §3's prune order. |
| **F3** | Client-controlled `operation_id` as filename and fold key | **Design change.** §4b: the id is always engine-minted `stmt-<uuid>` and is the only thing that reaches a path; the client string is a validated alias (`^[A-Za-z0-9._:-]{1,128}$`) keyed by `(session, client_op_id)` and never a fold key. Plus `OXIDANT_HISTORY_MAX_PER_SESSION` for the cap-eviction attack. |
| **F4** | fsync trilemma | **Design change.** §7 opens with the single-sentence guarantee (terminal state durable before the client is told; intermediate events lossy up to the interval) and picks option (a): the *response* awaits a oneshot ack under `OXIDANT_HISTORY_ACK_TIMEOUT_MS`, degrading to `history: degraded` on timeout. Channel semantics restated to match: `running` dropped-and-counted, `submitted`/`snapshot` never dropped or coalesced. |
| **F5** | Replay neutralized by `STATEMENT_TTL`; `Instant` not reconstructible | **Design change.** New §5b defines the hot/history two-tier model, states that replay populates the history tier and that `evict_expired` sweeps only the hot tier, and switches age arithmetic to the already-existing `submitted_at_ms`, keeping `Instant` only for live duration. Regression test added in §9. |
| **F6** | Size-roll and clock-roll collide | **Design change**, **shipped in PR3.** §3 "Naming" defines `oxidant-YYYY-MM-DD[-HH][.N]` with `.N` as the size-split sequence, chosen by scanning for the highest existing split so a restart mid-period is safe. `?file=` accepts `.N` (§6). |
| **F7** | Parquet-on-roll vs crash honesty; §3/§6 disagree | **Design change**, **shipped in PR3.** §6 rewritten: text is authoritative, conversion is a separate step *after* close+fsync+rename, the text file is unlinked only after the Parquet footer reads back, **a crash between roll and convert leaves the text file and the next boot converts it**, a `.parquet.tmp` is deleted and redone. §3 reserves conversion headroom against the budget and the free-space floor. §6 also makes the writer a real `tracing` layer with a `ts` column (the source lines had no timestamp) and states the lost-`grep` cost with the `OXIDANT_LOG_PARQUET=off` out. |
| **F8** | Default `on_pressure` cannot deliver Goal 2 | **Design change, both halves** — and **partly reverted in PR2**. The second half shipped as written: `OXIDANT_RESULT_MEMORY_BUDGET_BYTES` (512 MiB) is a real in-memory byte budget that `on_pressure` triggers on, and spill runs on a dedicated writer thread, never under the store mutex. The first half did not: the shipped default is **`on_pressure`**, so that enabling history does not by itself start writing every query's rows to disk. `always` is implemented and tested and is one env var away; §5 states the trade rather than hiding it. |
| **F9** | Event schema missing fields | **Design change.** §4a's schema carries `schema`, `error`, `submitted_at_ms`, `duration_ms`, `session`, `source`, `sql_encoding`, `seq`, `last_seq` and the result pointer. `seq` is defined as the writer-assigned submit sequence, stable across compaction. |
| **F10** | Compaction races, non-atomic swap, undefined replay order, no dir fsync | **Design change.** §4d: seal-before-compact (the writer never shares an open segment), the five-step swap with a `.done` marker and boot recovery, and mandatory parent-directory fsync for every rename in the design, with the explicit note that `checkpoint.rs` offers no precedent. §4c defines replay order twice over — a deterministic numerically-sorted file order *and* a seq-monotone fold that makes order irrelevant and double-folds harmless. |
| **F11** | Timezone/DST/ISO weeks | **Design change**, **shipped in PR3.** §3 pins all names to **UTC** and says so in the runtime contract; weekly is `%G-W%V` with a year-boundary test; `KEEP_DAYS` is defined against the file's *period* with weekly rounding up, stated as the operator contract. The chrono dependency note (workspace-internal, `features = ["std","clock"]`, `oxidant-loom`'s clockless pin) is in §2. |
| **F12** | `?file=` grammar/validation/authz | **Design change**, **shipped in PR3.** §6 gives the full grammar including weekly and `.N`, parses it into a typed `LogPeriod` and reconstructs the filename (never string-joins), answers `400` otherwise, lets the server choose the extension, and restates the `deny_unless_authorized` gate for `?file=` and every §6b endpoint. The U+2011 hyphen is fixed. |
| **F13** | Result files never GC'd against the journal | **Design change.** §5: pruning a statement unlinks its result in the same sweep, boot reconciles `results/` against the folded id set, and **the journal is named as the authority** — a result outlives its record by at most one sweep and never across a restart. |
| **F14** | Two processes share one data dir | **Design change.** §3c adds the exclusive `.lock` (flock, pid/role/port/root recorded) on the *effective statements dir* — not the root, which `OXIDANT_HISTORY_DIR` can point away from — the exact second-process error text, its root-disagreement variant, and `OXIDANT_DATA_DIR_PER_PROCESS=1` to derive `<root>/<role>-<port>/` instead of failing. |
| **F15** | 30 days of raw SQL and tracing fields, no mode or redaction | **Design change.** §3 "File modes and sensitivity": 0600 files / 0700 dirs set at create time, and `OXIDANT_HISTORY_SQL=text\|redacted\|hash` (default `text`, i.e. off) reusing the existing `store.rs` redaction, with the `hash` mode's API consequence stated rather than hidden. |
| **F16** | Budget exempts `event_log_dir` | **Design change**, deferred by PR2 and **shipped in PR3.** §3's budget covers it, and §8 replaces "stays untouched" with its own knob `OXIDANT_EVENT_LOG_MAX_BYTES` (2 GiB), rolling `events.jsonl` to periodised files rather than deleting the live one, with `0` restoring today's unbounded behaviour. The directory is billed to the budget **only while it is bounded**: under `=0` it is unprunable by the operator's own choice, and counting it would make the sweeper delete statement history to pay for it. Two implementation deviations (roll at half the cap; never prune the newest generation) are argued in §8. |
| **F17** | Durable exec logs cover the driver only | **Design change**, **shipped in PR3.** §6c hoists subscriber init out of `rest::router` into a process-level `logging::init(role, port)` that `run_worker` also calls; every node writes its own logs, **collection is per-node**, and the driver federates reads (§6b) rather than ingesting. Statement history stays driver-scoped and says so. |
| **F18** | Journal vocabulary diverges from the API | **Design change.** §4a uses the API's five values verbatim; §4e is the explicit mapping table. `finished`/`cancelled`/`state` are gone, and `interrupted` is **not** added as a status — a replayed non-terminal is `failed` + `error:"interrupted by restart"`, needing no contract change. |
| **F19** | Two claims stated as no-ops | **Design change.** §5 announces `410 result_expired` as a **new** status code and error string with the `404`/`409`/`400` behaviour it sits beside. §8 makes `OXIDANT_HISTORY=off` revert the caps too (1000 / 1 h), renames the knob to `OXIDANT_HISTORY_MAX_RECORDS` to avoid the const collision, and **documents the 10,000-vs-1,000 divergence explicitly** as an announced behaviour change with the knob to undo it. |
| **F20** | Layout notes fight existing conventions | **Design change.** §3 drops "next to checkpoints" entirely, defaults the root to `$XDG_DATA_HOME/oxidant` (or `/var/lib/oxidant` as a system service) with `OXIDANT_DATA_DIR` override, defines root-vs-subtree precedence for `OXIDANT_HISTORY_DIR`/`OXIDANT_LOG_DIR`/`OXIDANT_RESULT_DIR`, rejects object-store URLs loudly at boot, and states that `checkpoint.rs` is deliberately *not* this design's precedent because `PUT` gave it atomicity for free. |
| **F21** | Dedup needs a flush rule; diverges from the live tail | **Design change**, **shipped in PR3.** §6 flushes the `… repeated N times` summary on a different line, a **5 s timer**, a roll, or shutdown; and states the divergence plainly — dedup is file-only, the file is authoritative, and both `?file=current` and the SSE tail advertise their `dedup` state in the response. |
