# Adding workers

Oxidant runs **single-node by default**: with no workers registered, the driver executes every
query locally. That is the right setup for development and small data. Add workers when you
want distributed scans and shuffles across more CPU than one process has.

Check what mode a running server is in:

```sh
curl -s http://localhost:4040/api/v1/cluster/status
# {"mode":"single-node","workers":[],"version":"…"}
```

## Local cluster (in-process workers)

For single-host scale-out, the server can embed N Arrow Flight workers in the same process:

```sh
oxidant spark server --port 50051 --mode local-cluster --workers 4
```

Each worker listens on an ephemeral `127.0.0.1` port and the driver routes distributable
queries through them automatically. If `--workers` is omitted, the count falls back to
`OXIDANT_DEFAULT_PARALLELISM`, then to `2`. This mode is for development and CI — use separate
hosts (below) for real clusters.

## Multiple hosts

Run one driver plus a worker process on each additional machine.

On every worker host:

```sh
oxidant worker --port 50561
```

On the driver host, pass the static worker list at startup:

```sh
oxidant spark server --port 50051 --workers host1:50561,host2:50561
```

Equivalent alternatives:

- Environment: `OXIDANT_WORKERS=host1:50561,host2:50561`
- Per session, from any Spark Connect client: set the conf `spark.oxidant.workers` to the
  same comma-separated list.

**Registration is static.** The driver takes the worker set at startup; there is no dynamic
join/leave. To add, remove, or replace workers, restart the driver with the new list.

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

Static lists are the simple path. For Kubernetes, the driver can instead resolve live worker
endpoints from a headless Service / EndpointSlices (`OXIDANT_WORKER_SERVICE`), which tracks
autoscaling without driver restarts. That env contract is documented in
[`runtime-contract.md`](runtime-contract.md); a full EC2/ASG data plane (Packer AMI +
CloudFormation, Route53 discovery) is in [`distributed-ec2.md`](distributed-ec2.md).

## Notes

- Workers need the same catalog config as the driver when queries touch external catalogs —
  pass the same `--catalog-conf` / `OXIDANT_CATALOG_CONF` to `oxidant worker` (see
  [catalogs-glue.md](catalogs-glue.md)).
- Worker-side tuning env vars (task slots, spill, join strategy) are listed in
  [`runtime-contract.md`](runtime-contract.md).
