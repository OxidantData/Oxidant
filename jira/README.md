# Jira import artifacts — Databricks SQL parity epic

This directory contains the epic and stories from the plan in
[`../docs/databricks-parity-plan.md`](../docs/databricks-parity-plan.md), ready for import
into Jira.

> These stories have since been imported as **KAN-89..KAN-108** under epic **KAN-88**. The files
> are kept as the record of what was imported; see
> [`../docs/databricks-coverage.md`](../docs/databricks-coverage.md) §"Ticket map" for the
> row-order mapping onto the real Jira keys.

## Files

- `databricks-parity-tickets.csv` — Jira CSV import format (1 Epic + 20 Stories).
- `databricks-parity-tickets.json` — Structured JSON if you prefer scripted/API import.

## How to import

### Option A: Jira CSV import (UI)
1. Go to **Jira → Issues → Import issues from CSV**.
2. Upload `databricks-parity-tickets.csv`.
3. Map fields:
   - `Summary` → Summary
   - `Issue Type` → Issue Type
   - `Epic Link` → Epic Link
   - `Component` → Component(s)
   - `Labels` → Label(s)
   - `Description` → Description
   - `Acceptance Criteria` → a custom text field or append to Description
4. Replace the placeholder epic key `OXIDANT-DBR-PARITY` with the real epic key after the epic is created.

### Option B: Jira REST API
Use `databricks-parity-tickets.json` with a script or tool like `jira-cli` / `go-jira`:

```bash
export JIRA_API_TOKEN="your-token"
export JIRA_BASE_URL="https://your-domain.atlassian.net"
export JIRA_PROJECT="OXIDANT"
# Then run your importer against the JSON schema above.
```

### Note on the Epic key
`OXIDANT-DBR-PARITY` is a placeholder. Create the Epic first, then use its real issue key as the parent for the stories (replace `Epic Link` values in the CSV/JSON before import).
