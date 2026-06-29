# syntax=docker/dockerfile:1

# ---- Build stage ------------------------------------------------------------
# Pin the same Rust major as rust-toolchain.toml to keep the native ABI in sync.
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /build

# aws-lc-sys (reqwest's rustls crypto provider, used for HTTPS to Dawarich)
# compiles AWS-LC from C source, which needs cmake + a C toolchain.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies separately from source for faster rebuilds.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
# Touch so cargo rebuilds with the real sources, then build the final binary.
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

# ---- Runtime stage ----------------------------------------------------------
# Distroless cc image: glibc + libgcc only, no shell, no package manager.
# `nonroot` runs as uid/gid 65532. The binary is the only entrypoint.
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Link the GHCR package to this repo (enables retention + provenance/SBOM
# attestation resolution). metadata-action adds the rest of the OCI labels.
LABEL org.opencontainers.image.source="https://github.com/tgrecojr/locationrelay"

COPY --from=builder /build/target/release/locationrelay /usr/local/bin/locationrelay

# Data directory must be writable by uid 65532 — mount a volume owned by it.
ENV LOCATIONRELAY_BIND=0.0.0.0:8080 \
    LOCATIONRELAY_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8080

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/locationrelay"]

# There is no HTTP health endpoint by design (every unauthenticated path 404s).
# The distroless image also has no shell for a Docker HEALTHCHECK. Use a TCP
# liveness probe against the listen port (open socket = process alive), e.g. a
# Kubernetes tcpSocket probe or `nc -z host 8080`.
#
# Recommended hardened run (only /data needs to be writable):
#   docker run --read-only --tmpfs /tmp \
#     --cap-drop=ALL --security-opt=no-new-privileges \
#     -v locationrelay-data:/data ...
# The container already runs as non-root (uid 65532) via USER above.
