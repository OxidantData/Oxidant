# CI runner for the heavy ClickBench (`bench.yml`)

Light CI (fmt / clippy / test / coverage / parity) runs on **free GitHub-hosted runners** —
see `.github/workflows/ci.yml`. Nothing here is needed for that.

This directory provisions the **one** job that can't fit on a hosted runner: the real ClickBench
on the 14.78 GB `hits.parquet`, which needs a `c6a.4xlarge` (16 vCPU / 32 GB, ~26 GB spill pool).
It is consumed by `.github/workflows/bench.yml`, which is `workflow_dispatch`-only and **dormant**
until a runner with the `clickbench` label is registered.

## Recommended shape: ephemeral, on-demand, self-terminating

> You chose to **defer** this because it's expensive — so nothing here runs automatically.
> `launch-ephemeral-runner.sh` is the button to press *when* you want a run; it is never invoked
> by CI and never by these files. Estimated cost of one run: a `c6a.4xlarge` is ~$0.68/hr
> on-demand in us-west-2, and the box **terminates itself** the moment the job ends, so a
> 30–60 min benchmark is roughly **$0.35–0.70 per run, $0 idle**.

Flow:

1. You launch one `c6a.4xlarge` with `launch-ephemeral-runner.sh`.
2. Its `user_data` installs Rust, registers the box as an **ephemeral** runner
   (`--ephemeral --labels self-hosted,linux,x64,clickbench`), and runs `./run.sh`.
3. GitHub hands it exactly one queued `bench.yml` job; the ephemeral runner exits after that one
   job, `user_data` calls `shutdown -h now`, and because the instance is launched with
   `--instance-initiated-shutdown-behavior terminate`, the box **terminates** — no idle spend, no
   stale runner left registered.

So the usual order is: trigger `bench.yml` (Actions tab → "Run workflow") so a job is queued,
*then* launch the runner; it picks up the job, runs it, and self-destructs.

## One-time prerequisite you must do (I can't)

Registering a self-hosted runner needs a **registration token**, which needs **repo-admin**.
The `gh` CLI in this environment is authenticated as `kaicoder03`, which is *not* an admin of
`OxidantData/Oxidant` (the runners API returns 403), so this step is yours:

- **UI:** repo → Settings → Actions → Runners → "New self-hosted runner" → copy the token from the
  `./config.sh --token <TOKEN>` line, **or**
- **CLI (with your own admin PAT):**
  ```sh
  gh api -X POST repos/OxidantData/Oxidant/actions/runners/registration-token --jq .token
  ```

The token expires in ~1 hour and is single-use — mint it right before launching.

## Launch

```sh
export OXIDANT_RUNNER_TOKEN="<registration-token-from-above>"
./deploy/ci-runner/launch-ephemeral-runner.sh
```

Override any default inline, e.g. `REGION=us-west-2 INSTANCE_TYPE=c6a.4xlarge
VOLUME_GB=120 KEY_NAME=oxidant-platform ./deploy/ci-runner/launch-ephemeral-runner.sh`.

## Security — this is a public repo, treat the runner as hostile-input

GitHub's own guidance: **do not** put a self-hosted runner on a public repo without isolation —
a malicious pull request can run arbitrary code on it. This setup is built to be safe:

- **Never PR-triggered.** `bench.yml` is `workflow_dispatch` (and, if you enable it, `schedule`)
  only — fork PRs can never reach this runner.
- **Ephemeral.** A fresh box per job, destroyed after — no state survives between runs.
- **No credentials on the box.** No IAM instance profile, no repo secrets used by the job; the
  dataset is a public download. Nothing to steal.
- **Not the control plane.** Do **not** reuse the `oxidant-platform-control` EC2 box — it holds the
  gateway's JWT secret + cloud creds. Keep CADENCE-arbitrary build code far away from it.
- If you later add `pull_request` triggers, turn on "Require approval for all outside
  collaborators" (Settings → Actions → General) first.

## Teardown / safety net

Self-termination handles the happy path. If a launch wedges before the job runs, kill it by tag:

```sh
aws ec2 describe-instances --region "${REGION:-us-west-2}" \
  --filters Name=tag:Name,Values=oxidant-ci-runner Name=instance-state-name,Values=running,pending \
  --query 'Reservations[].Instances[].InstanceId' --output text
aws ec2 terminate-instances --region "${REGION:-us-west-2}" --instance-ids <id>
```

And remove a stale runner registration (if any) from repo → Settings → Actions → Runners.
