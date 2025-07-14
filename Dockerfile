ARG DEP_IMAGE=hyle-dep:arm64
ARG BASE_IMAGE=debian:bookworm-slim

FROM ${DEP_IMAGE} AS builder

WORKDIR /usr/src/hyle

# Build application
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml .cargo/config.toml
COPY src ./src
COPY crates ./crates
RUN cargo build --release -F sp1 -F risc0

# RUNNER
FROM ${BASE_IMAGE} AS runner

RUN apt-get update && apt-get install -y --no-install-recommends \
       ca-certificates curl libc6 binutils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /hyle

COPY --from=builder /usr/src/hyle/target/release/hyle               ./
COPY --from=builder /usr/src/hyle/target/release/indexer            ./
COPY --from=builder /usr/src/hyle/target/release/hyle-loadtest      ./
COPY --from=builder /usr/src/hyle/target/release/gcs_uploader       ./
COPY --from=builder /usr/src/hyle/target/release/smt_auto_prover    ./
COPY --from=builder /usr/src/hyle/target/release/nuke_tx            ./
COPY --from=builder /usr/src/hyle/target/release/health_check       ./
COPY --from=builder /usr/src/hyle/target/release/rate_limiter_proxy ./

VOLUME /hyle/data

EXPOSE 4321 1234

ENV HYLE_DATA_DIRECTORY=data
ENV HYLE_RUN_INDEXER=false
ENV HYLE_REST_ADDRESS=0.0.0.0:4321

CMD ["./hyle"]
