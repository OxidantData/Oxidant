# Drivers and workers

Oxidant runs **single-node by default**: with no workers registered, the driver executes every
query locally. That is the right setup for development and small data. Add workers when you
want distributed scans and shuffles across more CPU than one process has.

## Start the driver

Every install path (curl installer, brew, deb/rpm, Docker, source) ships the same `oxidant`
binary, and the driver is simply the Spark Connect server:

```sh
oxidant spark server --port 50051
```

(From a source build: `./target/debug/oxidant spark server --port 50051`.)

This starts Spark Connect gRPC on `50051` plus the Web UI / REST API on `4040`. With no
workers registered it executes every query itself — there is nothing else to set up.

Check what mode a running driver is in:

```sh
curl -s http://localhost:4040/api/v1/cluster/status
# {"mode":"single-node","workers":[],"version":"…"}
```

`mode` is `single-node` (no workers), `local-cluster` (in-process workers, below), or
`distributed` (remote workers attached).

## Local cluster (in-process workers)

For single-host scale-out, the server can embed N Arrow Flight workers in the same process:

```sh
oxidant spark server --port 50051 --mode local-cluster --workers 4
```

Each worker listens on an ephemeral `127.0.0.1` port and the driver routes distributable
queries through them automatically. If `--workers` is omitted, the count falls back to
`OXIDANT_DEFAULT_PARALLELISM`, then to `2`. This mode is for development and CI — use separate
hosts (below) for real clusters.

## Multiple hosts / bare metal

There is **no auto-join multicast and no Route53 requirement**. Bare metal, VMs, and
on-prem racks use the same model as Spark Standalone’s static executor list: start Flight
workers, then tell the driver their reachable `host:port` addresses.

On every worker host (use a unique shard index when you shard scans across machines):

```sh
export OXIDANT_SHARD_INDEX=0   # 0 .. N-1, distinct per worker
export OXIDANT_WORKER_COUNT=2
oxidant worker --port 50561
```

On the driver host, pass the static worker list (private/LAN IPs preferred):

```sh
export OXIDANT_DISTRIBUTED_STRICT=1   # fail closed if workers are unreachable
oxidant spark server --port 50051 --workers 10.0.0.11:50561,10.0.0.12:50561
```

The startup log confirms registration (`Oxidant static workers: http://10.0.0.11:50561,...`),
and the status endpoint then reports `"mode":"distributed"` with the worker list.

Equivalent alternatives:

- Environment: `OXIDANT_WORKERS=10.0.0.11:50561,10.0.0.12:50561`
- Per session: Spark conf `spark.oxidant.workers` (same comma-separated list; overrides
  the startup list for that session). It steers **query routing** only — the
  [log browser's](api.md#worker-logs-through-the-driver-workerid) `?worker=` and its diagnostic
  dump always resolve against `OXIDANT_WORKERS` / `OXIDANT_WORKER_SERVICE` / the startup list,
  because the Connect port that sets this key is unauthenticated and a log route that followed
  the pin would dial whatever address a client put in it.

**Security (bare metal):** bind Flight to a private interface; firewall **50561** so only the
driver (and peer workers for shuffle) can connect. Prefer VPC/VPN/WireGuard addresses over
opening Flight to the internet. Do **not** use UDP/LAN broadcast discovery.

**Registration is static.** To add, remove, or replace workers, restart the driver with the
new list (or update `spark.oxidant.workers`). EC2 ASG deployments automate that pin at boot
via IAM `Describe*` → private IPs (see [`distributed-ec2.md`](distributed-ec2.md)).

**Don't mix the two meanings of `--workers`.** In the default local mode it takes remote
endpoints (`host:port,...`); with `--mode local-cluster` it takes an in-process worker
*count*. A bare number without `--mode local-cluster` (or endpoints with it) is rejected at
startup with a hint, rather than silently ignored.

## Docker workers

The published image is the same `oxidant` binary for both roles — override the command:

```sh
# driver
docker run -p 50051:50051 -p 4040:4040 ghcr.io/oxidantdata/oxidant

# worker (on each worker host)
docker run ghcr.io/oxidantdata/oxidant worker --port 50561
```

Then start the driver with `--workers <host>:50561,...` as above. Image details, read-only
rootfs mounts, and build instructions: [`deploy/docker/README.md`](../deploy/docker/README.md).

## Kubernetes discovery

Static lists are the simple path (bare metal and most VMs). For Kubernetes, the driver can
instead resolve live worker endpoints from a headless Service (`OXIDANT_WORKER_SERVICE`).
That env contract is in [`runtime-contract.md`](runtime-contract.md). EC2/ASG (Packer AMI +
CloudFormation) pins `OXIDANT_WORKERS` from ASG private IPs — see
[`distributed-ec2.md`](distributed-ec2.md).

## Notes

- Workers need the same catalog config as the driver when queries touch external catalogs —
  pass the same `--catalog-conf` / `OXIDANT_CATALOG_CONF` to `oxidant worker` (see
  [catalogs-glue.md](catalogs-glue.md)).
- Worker-side tuning env vars (task slots, spill, join strategy) are listed in
  [`runtime-contract.md`](runtime-contract.md).
