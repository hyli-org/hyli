ARG DEP_IMAGE=hyli-dep
ARG BASE_IMAGE=ghcr.io/hyli-org/base:main
ARG ENABLE_CAIRO_M=false

FROM ${DEP_IMAGE} AS builder

# Re-declare ARG to make it available in this build stage
ARG ENABLE_CAIRO_M

WORKDIR /usr/src/hyli

# Build application
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml .cargo/config.toml
COPY src ./src
COPY crates ./crates
RUN if [ "${ENABLE_CAIRO_M}" = "true" ]; then \
    cargo +nightly-2025-04-06 build --release -F sp1 -F risc0 -F cairo-m -F rate-proxy; \
else \
    cargo build --release -F sp1 -F risc0 -F rate-proxy; \
fi

# RUNNER
FROM ${BASE_IMAGE} AS runner

WORKDIR /hyli

COPY --from=builder /usr/src/hyli/target/release/hyli               ./
COPY --from=builder /usr/src/hyli/target/release/indexer            ./
COPY --from=builder /usr/src/hyli/target/release/hyli-loadtest      ./
COPY --from=builder /usr/src/hyli/target/release/gcs_uploader       ./
COPY --from=builder /usr/src/hyli/target/release/smt_auto_prover    ./
COPY --from=builder /usr/src/hyli/target/release/health_check       ./
COPY --from=builder /usr/src/hyli/target/release/rate_limiter_proxy ./

VOLUME /hyli/data

EXPOSE 4321 1234

ENV HYLI_DATA_DIRECTORY=data
ENV HYLI_RUN_INDEXER=false
ENV HYLI_REST_ADDRESS=0.0.0.0:4321

CMD ["./hyli"]
