ARG DEP_IMAGE=hyle-dep
ARG BASE_IMAGE=ghcr.io/hyli-org/base:main

FROM $DEP_IMAGE AS builder

# Build application
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml .cargo/config.toml
COPY src ./src
COPY crates ./crates
RUN cargo build --release -F sp1 -F risc0 -F rate-proxy

# RUNNER
FROM $BASE_IMAGE 

WORKDIR /hyle

COPY --from=builder /usr/src/hyle/target/release/hyle ./
COPY --from=builder /usr/src/hyle/target/release/indexer ./
COPY --from=builder /usr/src/hyle/target/release/hyle-loadtest ./
COPY --from=builder /usr/src/hyle/target/release/gcs_uploader ./
COPY --from=builder /usr/src/hyle/target/release/smt_auto_prover ./
COPY --from=builder /usr/src/hyle/target/release/nuke_tx ./
COPY --from=builder /usr/src/hyle/target/release/health_check ./
COPY --from=builder /usr/src/hyle/target/release/node_state_check ./
COPY --from=builder /usr/src/hyle/target/release/rate_limiter_proxy ./

VOLUME /hyle/data

EXPOSE 4321 1234

ENV HYLE_DATA_DIRECTORY="data"
ENV HYLE_RUN_INDEXER="false"
ENV HYLE_REST_ADDRESS=0.0.0.0:4321

CMD ["./hyle"]
