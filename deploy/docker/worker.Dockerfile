# syntax=docker/dockerfile:1.7
###############################################################################
# Weft worker  —  `weft worker` (Arrow Flight shuffle worker)
#
# The `worker` subcommand lives in the SAME `weft` binary as the Spark Connect
# server (weft-cli's main dispatches `server` / `worker` / `driver`). Rather than
# compile the identical artifact a second time, the worker image IS the
# connect-server image with a different default command — so the two stay
# bit-for-bit identical and share every registry layer.
#
# The orchestrator (crates/weft-orchestrator/manifests.rs) runs the worker as:
#     command: ["weft"]  args: ["worker"]
# with the SAME hardened securityContext + emptyDir scratch as the driver, so the
# inherited non-root / read-only-rootfs posture from the base image is exactly right.
#
# Build context is the repo root; build AFTER connect-server exists in the registry
# (or locally):
#   docker build -f deploy/docker/worker.Dockerfile \
#     --build-arg CONNECT_IMAGE=weft/connect-server:<tag> -t weft/worker:<tag> .
#
# Prefer no second image at all? Drop this file and run the connect-server image
# with `command: ["weft"]  args: ["worker"]` (Helm `worker.command`). The
# orchestrator already does exactly that via WEFT_WORKER_IMAGE.
###############################################################################
ARG CONNECT_IMAGE=weft/connect-server:latest
FROM ${CONNECT_IMAGE}

LABEL org.opencontainers.image.title="weft-worker" \
      org.opencontainers.image.description="Weft Arrow Flight worker (same weft binary as connect-server)"

# Default Flight worker port (the orchestrator's StatefulSet supplies its own args).
# Note: `weft worker` requires --port, so the standalone default includes it.
EXPOSE 50561

# Inherits from the connect-server base:
#   USER 65532:65532, /usr/local/bin/weft ENTRYPOINT, TMPDIR/HOME=/tmp,
#   and the read-only-rootfs posture. Helm sets WEFT_SHUFFLE_SPILL_DIR on the
#   spill volume (WEFT_SPILL_DIR is unused legacy).
CMD ["worker", "--port", "50561"]
