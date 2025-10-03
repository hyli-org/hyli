# Need full image for openSSL build
ARG DEP_IMAGE=rust:bookworm
# Preloaded with bb and risc0 (on x86)
ARG BASE_IMAGE=ghcr.io/hyli-org/base:main

FROM ${DEP_IMAGE} AS builders

WORKDIR /usr/src/hyli

# Build application
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml .cargo/config.toml
COPY src ./src
COPY crates ./crates
RUN cargo build --release -F sp1 -F risc0;

# RUNNER
FROM ${BASE_IMAGE} AS runner

WORKDIR /hyli

COPY --from=builder /usr/src/hyli/target/release/hyli               ./
COPY --from=builder /usr/src/hyli/target/release/indexer            ./
COPY --from=builder /usr/src/hyli/target/release/hyli-loadtest      ./
COPY --from=builder /usr/src/hyli/target/release/gcs_uploader       ./
COPY --from=builder /usr/src/hyli/target/release/smt_auto_prover    ./
COPY --from=builder /usr/src/hyli/target/release/health_check       ./

VOLUME /hyli/data

EXPOSE 4321 1234

ENV HYLI_DATA_DIRECTORY=data
ENV HYLI_RUN_INDEXER=false
ENV HYLI_REST_ADDRESS=0.0.0.0:4321

CMD ["./hyli"]
