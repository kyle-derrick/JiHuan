#!/usr/bin/env bash
# Build a minimal Docker image for jihuan-server
set -euo pipefail

IMAGE_NAME="${JIHUAN_IMAGE:-jihuan-server}"
IMAGE_TAG="${JIHUAN_TAG:-latest}"

cat > /tmp/jihuan.Dockerfile <<'EOF'
# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM rust:1.75-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN cargo build --release -p jihuan-server -p jihuan-cli

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/jihuan-server /usr/local/bin/
COPY --from=builder /build/target/release/jihuan /usr/local/bin/
COPY --from=builder /build/config/ /app/config/

ENV JIHUAN_DATA_DIR=/data
ENV JIHUAN_HTTP_ADDR=0.0.0.0:8080
ENV JIHUAN_GRPC_ADDR=0.0.0.0:8081

VOLUME ["/data"]
EXPOSE 8080 8081 9090

ENTRYPOINT ["jihuan-server"]
EOF

echo "Building ${IMAGE_NAME}:${IMAGE_TAG} ..."
docker build -f /tmp/jihuan.Dockerfile -t "${IMAGE_NAME}:${IMAGE_TAG}" .
echo "Done: ${IMAGE_NAME}:${IMAGE_TAG}"
