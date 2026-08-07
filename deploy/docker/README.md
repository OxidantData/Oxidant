# Oxidant container images

Production Dockerfiles for the Oxidant engine images. Everything builds from the Cargo
workspace at the repository root; the **build context for every image is the repo
root**, e.g. `docker build -f deploy/docker/<name>.Dockerfile -t <ref> .`.

The multi-arch image is published to GHCR as `ghcr.io/oxidantdata/oxidant` (see
`.github/workflows/oxidant-image.yml`). A runnable EC2/ASG deployment is documented in
[`docs/distributed-ec2.md`](../../docs/distributed-ec2.md).

| File | Image | Binary / crate | Entry | Port |
|------|-------|----------------|-------|------|
| `connect-server.Dockerfile` | `connect-server` | `oxidant` (crate `oxidant-cli`) | `oxidant spark server --port 50051` | 50051 (gRPC), 4040 (HTTP: UI + REST API) |
| `worker.Dockerfile` | `worker` | `oxidant` (same binary, rebased on `connect-server`) | `oxidant worker` | 50561 (Flight) |
| `gateway.Dockerfile` | `gateway` *(not in tree yet)* | `oxidant-gateway` + kubectl + SPA | `oxidant-gateway` | 8080 |

> The `worker` and `connect-server` images are the **same `oxidant` binary** — `worker`
> is `connect-server` with a different default command, so they share every layer.
> You may run `connect-server` for both roles and override the worker command.

## Build

```sh
# from the repo root
TAG=$(git rev-parse --short HEAD)

# 1) Spark Connect driver (also the source image for the worker)
docker build -f deploy/docker/connect-server.Dockerfile \
  -t oxidant/connect-server:$TAG .

# 2) Worker — rebases connect-server, no recompile
docker build -f deploy/docker/worker.Dockerfile \
  --build-arg CONNECT_IMAGE=oxidant/connect-server:$TAG \
  -t oxidant/worker:$TAG .

# Confirm AWS CLI v2 is bundled (required for Glue catalog shell-outs)
docker run --rm --entrypoint /usr/local/aws-cli/v2/current/bin/aws \
  oxidant/connect-server:$TAG --version
```

BuildKit is required (the `# syntax=` directive + the per-Dockerfile
`*.dockerignore` files in this directory). Use `docker buildx` for multi-arch
(`--platform linux/amd64,linux/arm64`); the AWS CLI stage selects the zip by
`TARGETARCH`.

### How the build stages work

- **Rust:** `rust:1.90-bookworm` (matches `rust-toolchain.toml`) + `cargo-chef` for
  dependency-layer caching across source edits. No `protoc` is needed — `oxidant-proto`
  compiles the vendored Spark Connect protos with pure-Rust `protox`.
- **AWS CLI:** a dedicated `awscli` stage installs AWS CLI v2 from Amazon’s official
  zip into `/usr/local/aws-cli`. Runtime sets
  `OXIDANT_AWS_BIN=/usr/local/aws-cli/v2/current/bin/aws`.
- **Runtime:** `debian:bookworm-slim` (not distroless) — the AWS CLI v2 bundle needs
  shared libraries distroless omits. User/group **65532** (`nonroot`).
- **Credentials:** none are baked into the image. In-cluster auth is IRSA / env /
  instance role; the CLI is only the binary Glue catalog resolution shells out to.

## Security posture

Images target PodSecurity **`restricted`** (as stamped by the orchestrator):

| Control | How the image satisfies it |
|---------|----------------------------|
| `runAsNonRoot` / `runAsUser: 65532` | `USER 65532:65532`; binary at `/usr/local/bin/oxidant` |
| `readOnlyRootFilesystem: true` | scratch only on mounted emptyDirs |
| `capabilities.drop: ["ALL"]` | plain TCP listener |
| `allowPrivilegeEscalation: false` | no setuid binaries |
| `seccompProfile: RuntimeDefault` | default profile |
| No baked credentials | IRSA / external identity; AWS CLI binary only |

### Read-only rootfs ⇒ emptyDir scratch is mandatory

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
containers:
- name: connect
  securityContext:
    readOnlyRootFilesystem: true
    allowPrivilegeEscalation: false
    capabilities: { drop: ["ALL"] }
    seccompProfile: { type: RuntimeDefault }
  volumeMounts:
  - { name: tmp,   mountPath: /tmp }
  - { name: spill, mountPath: /var/lib/oxidant/spill }
volumes:
- { name: tmp,   emptyDir: {} }
- { name: spill, emptyDir: {} }
```

The image defaults `TMPDIR=/tmp` and `HOME=/tmp`. Point `TMPDIR` at
`/var/lib/oxidant/spill` (PVC or emptyDir) so threshold shuffle spill
(`OXIDANT_SHUFFLE_SPILL_BYTES` / `OXIDANT_MEMORY_LIMIT_BYTES`) lands on that volume via
`default_spill_root()`. **`OXIDANT_SHUFFLE_SPILL_DIR` is debug-only** — when set, the engine
force-spills every shuffle bucket to disk and invalidates benchmark timings. The
historical `OXIDANT_SPILL_DIR` env var is **unused**. Set `OXIDANT_MEMORY_LIMIT_BYTES` /
`OXIDANT_SHUFFLE_SPILL_BYTES` from the pod memory limit for threshold spill.

Standalone:

```sh
docker run --read-only --tmpfs /tmp --tmpfs /var/lib/oxidant/spill \
  -p 50051:50051 -p 4040:4040 oxidant/connect-server:$TAG
```

## Running the published image

The multi-arch engine image is published to GHCR as `ghcr.io/oxidantdata/oxidant`.
One image covers both roles — override the command per role:

```sh
# Spark Connect driver (gRPC on 50051; monitoring UI, SQL editor, and REST API
# at http://localhost:4040)
docker run --read-only --tmpfs /tmp --tmpfs /var/lib/oxidant/spill \
  -p 50051:50051 -p 4040:4040 ghcr.io/oxidantdata/oxidant:latest

# Arrow Flight worker (same image, different command)
docker run --read-only --tmpfs /tmp --tmpfs /var/lib/oxidant/spill \
  ghcr.io/oxidantdata/oxidant:latest worker --port 50561
```

Set `OXIDANT_WORKER_SERVICE` on the driver so it can discover workers (see
[`docs/runtime-contract.md`](../../docs/runtime-contract.md) for the full env
contract). An EC2/ASG deployment via CloudFormation is documented in
[`docs/distributed-ec2.md`](../../docs/distributed-ec2.md).

---

`gateway.Dockerfile`, `clustermgr`, `scheduler`, and `pyworker` images are not part of
this minimal set yet — add them as the control-plane crates leave skeleton state.
