# syntax=docker/dockerfile:1.7
###############################################################################
# Weft Spark Connect server  —  `weft spark server --port 50051`
#
# This is the per-user "cluster" driver pod image. The control plane materializes
# it via crates/weft-orchestrator (see manifests.rs): the container runs
#     command: ["weft"]  args: ["spark","server","--port","50051"]
# under PodSecurity `restricted` admission — runAsNonRoot, readOnlyRootFilesystem,
# drop ALL capabilities, seccomp=RuntimeDefault, no auto-mounted ServiceAccount token.
#
# The image is therefore built to:
#   * run as a fixed non-root uid (65532, == orchestrator RUN_AS),
#   * tolerate a read-only root filesystem — the ONLY writable paths are the
#     emptyDir mounts the manifest provides (see "Read-only rootfs" at the bottom),
#   * carry NO cloud credentials: catalog/storage access is per-cluster IRSA.
#
# Build context is the repository root (the Cargo workspace):
#   docker build -f deploy/docker/connect-server.Dockerfile -t weft/connect-server:<tag> .
###############################################################################

# ---- chef: pin the toolchain + install cargo-chef once ----------------------
# rust:1.90 matches rust-toolchain.toml. The full (non-slim) image already has the
# C toolchain that ring + zstd-sys compile against; the workspace needs no protoc
# (weft-proto compiles the vendored Spark protos with pure-Rust `protox`).
FROM rust:1.90-bookworm AS chef
WORKDIR /build
RUN cargo install cargo-chef --locked --version ^0.1

# ---- planner: capture the dependency graph (cache key is just the manifests) --
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: cook deps (cached across source edits), then build `weft` -------
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
# The `weft` binary lives in the `weft-cli` crate ([[bin]] name = "weft").
RUN cargo build --release --locked -p weft-cli --bin weft \
 && strip target/release/weft
# Pre-create the spill mount-point owned by the runtime uid so the image also works
# under `docker run --read-only` with tmpfs/volume mounts (K8s emptyDir handles this
# in-cluster via fsGroup). /tmp already exists in the distroless base.
RUN install -d -o 65532 -g 65532 /rootfs/var/lib/weft/spill

# ---- awscli: the engine resolves Glue (and HMS) catalogs by shelling out to `aws`
# (weft-catalog-glue). Bundle it so a *cluster* can list/read the catalog itself —
# arch-correct via TARGETARCH (don't default it; that pinned amd64 on arm64 builds).
FROM debian:bookworm-slim AS awscli
ARG TARGETARCH
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl unzip ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    case "${TARGETARCH}" in amd64) A=x86_64 ;; arm64) A=aarch64 ;; *) echo "bad arch ${TARGETARCH}"; exit 1 ;; esac; \
    curl -fsSLo /tmp/awscli.zip "https://awscli.amazonaws.com/awscli-exe-linux-${A}.zip"; \
    unzip -q /tmp/awscli.zip -d /tmp; \
    /tmp/aws/install -i /usr/local/aws-cli -b /usr/local/bin; \
    /usr/local/bin/aws --version

# ---- runtime: debian-slim (the AWS CLI v2 bundle needs libs distroless omits) ---
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -g 65532 nonroot \
 && useradd -u 65532 -g 65532 -m -d /home/nonroot -s /usr/sbin/nologin nonroot

LABEL org.opencontainers.image.title="weft-connect-server" \
      org.opencontainers.image.description="Weft Spark Connect server (per-user cluster driver)" \
      org.opencontainers.image.source="https://gitlab.com/weftlabs/weft"

# Spark Connect gRPC endpoint. Point PySpark at sc://<service>:50051.
EXPOSE 50051

# PodSecurity `restricted`: a fixed non-root uid that is never 0. distroless
# `nonroot` is uid/gid 65532, matching the orchestrator's RUN_AS.
USER 65532:65532

# Read-only rootfs survival: the engine stages sort/aggregation spill and Delta/
# catalog scratch under std::env::temp_dir() (== $TMPDIR). Keep every writable path
# on an emptyDir mount, and keep HOME off the read-only rootfs.
#   - WEFT_SPILL_DIR mirrors the orchestrator manifest (dedicated spill volume).
#   - WEFT_MEMORY_LIMIT_BYTES (unset here) bounds the spill pool; set it from the
#     pod's memory limit to make aggregations spill instead of OOM-killing.
ENV TMPDIR=/tmp \
    HOME=/tmp \
    WEFT_SPILL_DIR=/var/lib/weft/spill \
    WEFT_AWS_BIN=/usr/local/aws-cli/v2/current/bin/aws \
    RUST_BACKTRACE=1

COPY --from=builder /build/target/release/weft /usr/local/bin/weft
COPY --from=awscli /usr/local/aws-cli /usr/local/aws-cli
COPY --from=builder --chown=65532:65532 /rootfs/var/lib/weft/spill /var/lib/weft/spill

# Default command; the orchestrator overrides command/args per cluster but keeps
# this exact invocation.
ENTRYPOINT ["/usr/local/bin/weft"]
CMD ["spark", "server", "--port", "50051"]

###############################################################################
# Read-only rootfs — required writable mounts (provided by the orchestrator):
#
#   securityContext (pod):       runAsNonRoot, runAsUser/Group/fsGroup: 65532
#   securityContext (container): readOnlyRootFilesystem: true
#                                allowPrivilegeEscalation: false
#                                capabilities.drop: ["ALL"]
#                                seccompProfile.type: RuntimeDefault
#   volumes (emptyDir):
#     - name: tmp    mountPath: /tmp                 # $TMPDIR scratch + actual spill
#     - name: spill  mountPath: /var/lib/weft/spill  # WEFT_SPILL_DIR (dedicated vol)
#
# Standalone (outside K8s):
#   docker run --read-only \
#     --tmpfs /tmp --tmpfs /var/lib/weft/spill \
#     -p 50051:50051 weft/connect-server:<tag>
#
# Credentials: NONE are baked in. In-cluster, catalog/storage auth is per-cluster
# least-privilege IRSA (the pod's ServiceAccount role). The AWS CLI *binary* is
# bundled (see awscli stage + WEFT_AWS_BIN) so Glue catalog resolution can shell
# out to `aws glue …`; identity still comes from IRSA / the environment.
###############################################################################
