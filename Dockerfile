# Need full image for openSSL build
ARG DEP_IMAGE=rust:bookworm
# Preloaded with bb and risc0 (on x86)
ARG BASE_IMAGE=ghcr.io/hyli-org/base:main
# Pre-built verifier worker images
ARG SP1_VERIFIER_IMAGE=ghcr.io/hyli-org/hyli-sp1-verifier:main
ARG RISC0_VERIFIER_IMAGE=ghcr.io/hyli-org/hyli-risc0-verifier:main
ARG JOLT_VERIFIER_IMAGE=ghcr.io/hyli-org/hyli-jolt-verifier:main
ARG RETH_VERIFIER_IMAGE=ghcr.io/hyli-org/hyli-reth-verifier:main

FROM ${SP1_VERIFIER_IMAGE} AS sp1-verifier
FROM ${RISC0_VERIFIER_IMAGE} AS risc0-verifier
FROM ${JOLT_VERIFIER_IMAGE} AS jolt-verifier
FROM ${RETH_VERIFIER_IMAGE} AS reth-verifier

FROM ${DEP_IMAGE} AS builder

WORKDIR /usr/src/hyli

# Build application
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml .cargo/config.toml
COPY src ./src
COPY crates ./crates
RUN cargo build --release -F risc0 -F rate-proxy;

# RUNNER
FROM ${BASE_IMAGE} AS runner

WORKDIR /hyli

COPY --from=builder /usr/src/hyli/target/release/hyli                      ./
COPY --from=builder /usr/src/hyli/target/release/indexer                   ./
COPY --from=builder /usr/src/hyli/target/release/hyli-loadtest             ./
COPY --from=builder /usr/src/hyli/target/release/smt_long_running_test     ./
COPY --from=builder /usr/src/hyli/target/release/gcs_uploader              ./
COPY --from=builder /usr/src/hyli/target/release/smt_auto_prover           ./
COPY --from=builder /usr/src/hyli/target/release/health_check              ./
COPY --from=builder /usr/src/hyli/target/release/rate_limiter_proxy        ./
COPY --from=sp1-verifier   /hyli-sp1-verifier                             ./
COPY --from=risc0-verifier /hyli-risc0-verifier                            ./
COPY --from=jolt-verifier  /hyli-jolt-verifier                             ./
COPY --from=reth-verifier  /hyli-reth-verifier                             ./

VOLUME /hyli/data

EXPOSE 4321 1234

ENV HYLI_DATA_DIRECTORY=/hyli/data/node
ENV HYLI_RUN_INDEXER=false
ENV HYLI_REST_ADDRESS=0.0.0.0:4321
ENV PATH="/hyli:$PATH" 

CMD ["./hyli"]
