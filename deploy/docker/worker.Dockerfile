# syntax=docker/dockerfile:1.7
###############################################################################
# Oxidant worker  —  `oxidant worker` (Arrow Flight shuffle worker)
#
# The `worker` subcommand lives in the SAME `oxidant` binary as the Spark Connect
# server (oxidant-cli's main dispatches `server` / `worker` / `driver`). Rather than
# compile the identical artifact a second time, the worker image IS the
# connect-server image with a different default command — so the two stay
# bit-for-bit identical and share every registry layer.
#
# The orchestrator (crates/oxidant-orchestrator/manifests.rs) runs the worker as:
#     command: ["oxidant"]  args: ["worker","--foreground"]
# with the SAME hardened securityContext + emptyDir scratch as the driver, so the
# inherited non-root / read-only-rootfs posture from the base image is exactly right.
#
# Build context is the repo root; build AFTER connect-server exists in the registry
# (or locally):
#   docker build -f deploy/docker/worker.Dockerfile \
#     --build-arg CONNECT_IMAGE=oxidant/connect-server:<tag> -t oxidant/worker:<tag> .
#
# Prefer no second image at all? Drop this file and run the connect-server image
# with `command: ["oxidant"]  args: ["worker","--foreground"]`. The
# orchestrator already does exactly that via OXIDANT_WORKER_IMAGE.
###############################################################################
ARG CONNECT_IMAGE=oxidant/connect-server:latest
FROM ${CONNECT_IMAGE}

LABEL org.opencontainers.image.title="oxidant-worker" \
      org.opencontainers.image.description="Oxidant Arrow Flight worker (same oxidant binary as connect-server)"

# Default Flight worker port (the orchestrator's StatefulSet supplies its own args).
# Note: `oxidant worker` requires --port, so the standalone default includes it. --foreground
# because the container runtime is the supervisor and PID 1 must be the worker itself.
EXPOSE 50561

# Inherits from the connect-server base:
#   USER 65532:65532, /usr/local/bin/oxidant ENTRYPOINT, TMPDIR/HOME=/tmp,
#   and the read-only-rootfs posture. Deployments should set TMPDIR to the spill volume and
#   OXIDANT_SHUFFLE_SPILL_BYTES for threshold spill (OXIDANT_SHUFFLE_SPILL_DIR is
#   force-spill / debug-only; OXIDANT_SPILL_DIR is unused legacy).
CMD ["worker", "--port", "50561", "--foreground"]
