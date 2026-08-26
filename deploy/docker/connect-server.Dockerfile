# syntax=docker/dockerfile:1.7
###############################################################################
# Oxidant Spark Connect server  —  `oxidant spark server --port 50051 --foreground`
#
# This is the per-user "cluster" driver pod image. The control plane materializes
# it via crates/oxidant-orchestrator (see manifests.rs): the container runs
#     command: ["oxidant"]  args: ["spark","server","--port","50051","--foreground"]
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
#   docker build -f deploy/docker/connect-server.Dockerfile -t oxidant/connect-server:<tag> .
###############################################################################

# Cargo profile to compile with. `release-ci` (see the root Cargo.toml) drops LTO
# and raises codegen-units, which removes the serialized whole-program link that
# dominates a full build. Tagged releases override this back to `release`.
ARG CARGO_PROFILE=release-ci

# ---- chef: pin the toolchain + cargo-chef -----------------------------------
# rust:1.90 matches rust-toolchain.toml. The full (non-slim) image already has the
# C toolchain that ring + zstd-sys compile against; the workspace needs no protoc
# (oxidant-proto compiles the vendored Spark protos with pure-Rust `protox`).
# cargo-chef comes from its own published image rather than `cargo install`, which
# compiles it from source on every cold cache.
FROM lukemathwalker/cargo-chef:0.1.73-rust-1.90-bookworm AS chef
WORKDIR /build

# ---- planner: capture the dependency graph (cache key is just the manifests) --
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: cook deps (cached across source edits), then build `oxidant` -------
FROM chef AS builder
ARG CARGO_PROFILE
COPY --from=planner /build/recipe.json recipe.json
# Scoped to the one crate we actually ship: an unscoped cook builds dependencies
# for all 24 workspace members. Must use the same profile as the build below, or
# the cooked artifacts land in a different target dir and get recompiled.
RUN cargo chef cook --profile "$CARGO_PROFILE" --locked --recipe-path recipe.json -p oxidant-cli --bin oxidant
COPY . .
# The `oxidant` binary lives in the `oxidant-cli` crate ([[bin]] name = "oxidant").
RUN cargo build --profile "$CARGO_PROFILE" --locked -p oxidant-cli --bin oxidant \
 && strip "target/$CARGO_PROFILE/oxidant" \
 && install -D "target/$CARGO_PROFILE/oxidant" /out/oxidant
# Pre-create the spill mount-point owned by the runtime uid so the image also works
# under `docker run --read-only` with tmpfs/volume mounts (K8s emptyDir handles this
# in-cluster via fsGroup). /tmp already exists in the distroless base.
RUN install -d -o 65532 -g 65532 /rootfs/var/lib/oxidant/spill

# ---- awscli: bundled for operator scripts / in-image debugging only. The engine
# talks to Glue in-process via aws-sdk-glue (oxidant-catalog-glue) — no CLI shell-out.
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

LABEL org.opencontainers.image.title="oxidant-connect-server" \
      org.opencontainers.image.description="Oxidant Spark Connect server (per-user cluster driver)" \
      org.opencontainers.image.source="https://github.com/OxidantData/Oxidant"

# Spark Connect gRPC endpoint. Point PySpark at sc://<service>:50051.
EXPOSE 50051

# HTTP endpoint: monitoring UI, SQL editor/notebook, and the REST statement API
# (http://<host>:4040). Disabled when the container is run with `--no-ui`.
EXPOSE 4040

# PodSecurity `restricted`: a fixed non-root uid that is never 0. distroless
# `nonroot` is uid/gid 65532, matching the orchestrator's RUN_AS.
USER 65532:65532

# Read-only rootfs survival: the engine stages sort/aggregation spill and Delta/
# catalog scratch under std::env::temp_dir() (== $TMPDIR). Keep every writable path
# on an emptyDir mount, and keep HOME off the read-only rootfs.
#   - Shuffle spill: set OXIDANT_SHUFFLE_SPILL_BYTES (threshold) and point TMPDIR at a
#     writable spill volume. OXIDANT_SHUFFLE_SPILL_DIR is DEBUG-ONLY (force-spill).
#     The old OXIDANT_SPILL_DIR name is unused by the engine — do not set it.
#   - OXIDANT_MEMORY_LIMIT_BYTES (unset here) bounds the spill pool; set it from the
#     pod's memory limit to make aggregations spill instead of OOM-killing.
ENV TMPDIR=/tmp \
    HOME=/tmp \
    OXIDANT_SAMPLE_DATA_DIR=/opt/oxidant/sample-data \
    RUST_BACKTRACE=1

COPY --from=builder /out/oxidant /usr/local/bin/oxidant
COPY --from=awscli /usr/local/aws-cli /usr/local/aws-cli
COPY --from=builder --chown=65532:65532 /rootfs/var/lib/oxidant/spill /var/lib/oxidant/spill

# Bundled TPC-H sample data (parquet/csv/delta/iceberg, ~19 MB). The engine registers it as
# the `samples` schema at boot (OXIDANT_SAMPLE_DATA_DIR above), so a first-time user can open
# the UI and query `samples.tpch_nation` immediately. Read-only data; delete it (or unset the
# env) to slim a production image.
COPY --chown=65532:65532 sample-data/ /opt/oxidant/sample-data/

# Default command; the orchestrator overrides command/args per cluster but keeps
# this exact invocation.
ENTRYPOINT ["/usr/local/bin/oxidant"]
# --foreground: the container runtime (docker, kubelet) is the supervisor, so PID 1 has to
# BE the server. `oxidant start` would fork away and the container would exit immediately.
CMD ["spark", "server", "--port", "50051", "--foreground"]

###############################################################################
# Read-only rootfs — required writable mounts (provided by the orchestrator):
#
#   securityContext (pod):       runAsNonRoot, runAsUser/Group/fsGroup: 65532
#   securityContext (container): readOnlyRootFilesystem: true
#                                allowPrivilegeEscalation: false
#                                capabilities.drop: ["ALL"]
#                                seccompProfile.type: RuntimeDefault
#   volumes (emptyDir or PVC):
#     - name: tmp    mountPath: /tmp                 # $TMPDIR scratch
#     - name: spill  mountPath: /var/lib/oxidant/spill  # TMPDIR for threshold spill
#
# Standalone (outside K8s):
#   docker run --read-only \
#     --tmpfs /tmp --tmpfs /var/lib/oxidant/spill \
#     -p 50051:50051 oxidant/connect-server:<tag>
#
# Credentials: NONE are baked in. In-cluster, catalog/storage auth is per-cluster
# least-privilege IRSA (the pod's ServiceAccount role). The AWS CLI *binary* is
# bundled (see awscli stage) for operator scripts only — the engine talks to Glue
# in-process via aws-sdk-glue; identity comes from IRSA / the environment.
###############################################################################
