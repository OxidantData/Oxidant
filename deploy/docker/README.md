# Weft container images

Production Dockerfiles for the Weft engine images. Everything builds from the Cargo
workspace at the repository root; the **build context for every image is the repo
root**, e.g. `docker build -f deploy/docker/<name>.Dockerfile -t <ref> .`.

Runnable distributed deploy (Kind / BYO EKS) is documented in
[`docs/distributed-k8s.md`](../../docs/distributed-k8s.md). Helm chart:
[`deploy/helm/weft/`](../helm/weft/).

| File | Image | Binary / crate | Entry | Port |
|------|-------|----------------|-------|------|
| `connect-server.Dockerfile` | `connect-server` | `weft` (crate `weft-cli`) | `weft spark server --port 50051` | 50051 (gRPC) |
| `worker.Dockerfile` | `worker` | `weft` (same binary, rebased on `connect-server`) | `weft worker` | 50561 (Flight) |
| `gateway.Dockerfile` | `gateway` *(not in tree yet)* | `weft-gateway` + kubectl + SPA | `weft-gateway` | 8080 |

> The `worker` and `connect-server` images are the **same `weft` binary** — `worker`
> is `connect-server` with a different default command, so they share every layer.
> You may point both Helm refs at `connect-server` and override the worker command.

## Build

```sh
# from the repo root
TAG=$(git rev-parse --short HEAD)

# 1) Spark Connect driver (also the source image for the worker)
docker build -f deploy/docker/connect-server.Dockerfile \
  -t weft/connect-server:$TAG .

# 2) Worker — rebases connect-server, no recompile
docker build -f deploy/docker/worker.Dockerfile \
  --build-arg CONNECT_IMAGE=weft/connect-server:$TAG \
  -t weft/worker:$TAG .

# Confirm AWS CLI v2 is bundled (required for Glue catalog shell-outs)
docker run --rm --entrypoint /usr/local/aws-cli/v2/current/bin/aws \
  weft/connect-server:$TAG --version
```

BuildKit is required (the `# syntax=` directive + the per-Dockerfile
`*.dockerignore` files in this directory). Use `docker buildx` for multi-arch
(`--platform linux/amd64,linux/arm64`); the AWS CLI stage selects the zip by
`TARGETARCH`.

### How the build stages work

- **Rust:** `rust:1.90-bookworm` (matches `rust-toolchain.toml`) + `cargo-chef` for
  dependency-layer caching across source edits. No `protoc` is needed — `weft-proto`
  compiles the vendored Spark Connect protos with pure-Rust `protox`.
- **AWS CLI:** a dedicated `awscli` stage installs AWS CLI v2 from Amazon’s official
  zip into `/usr/local/aws-cli`. Runtime sets
  `WEFT_AWS_BIN=/usr/local/aws-cli/v2/current/bin/aws`.
- **Runtime:** `debian:bookworm-slim` (not distroless) — the AWS CLI v2 bundle needs
  shared libraries distroless omits. User/group **65532** (`nonroot`).
- **Credentials:** none are baked into the image. In-cluster auth is IRSA / env /
  instance role; the CLI is only the binary Glue catalog resolution shells out to.

## Security posture

Images target PodSecurity **`restricted`** (as stamped by Helm / the orchestrator):

| Control | How the image satisfies it |
|---------|----------------------------|
| `runAsNonRoot` / `runAsUser: 65532` | `USER 65532:65532`; binary at `/usr/local/bin/weft` |
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
  - { name: spill, mountPath: /var/lib/weft/spill }
volumes:
- { name: tmp,   emptyDir: {} }
- { name: spill, emptyDir: {} }
```

The image sets `TMPDIR=/tmp`, `HOME=/tmp`, and `WEFT_SPILL_DIR=/var/lib/weft/spill`.
Set `WEFT_MEMORY_LIMIT_BYTES` from the pod memory limit so large aggregations spill.

Standalone:

```sh
docker run --read-only --tmpfs /tmp --tmpfs /var/lib/weft/spill \
  -p 50051:50051 weft/connect-server:$TAG
```

## Mapping to Helm (`deploy/helm/weft`)

Minimal data-plane values (gateway off by default):

```yaml
connect:
  enabled: true
  image: <registry>/weft/connect-server:<tag>
worker:
  image: <registry>/weft/worker:<tag>
  replicas: 2
gateway:
  enabled: false
```

The chart wires `WEFT_WORKER_SERVICE=weft-worker.<ns>.svc.cluster.local` on the
driver and ships the emptyDir / securityContext mounts above. See
[`docs/distributed-k8s.md`](../../docs/distributed-k8s.md).

---

`gateway.Dockerfile`, `clustermgr`, `scheduler`, and `pyworker` images are not part of
this minimal set yet — add them as the control-plane crates leave skeleton state.
