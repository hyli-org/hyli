# Local OTLP Collector + VictoriaMetrics

This spins up VictoriaMetrics + OpenTelemetry Collector + Grafana for local metrics.

## Start

```bash
docker compose -f tools/observability/docker-compose.yml up -d
```

## Open UI

VictoriaMetrics UI:
`http://localhost:8428/vmui/`

## Run Hyle (example)

```bash
CARGO_AUTO_INIT_OTLP_GLOBAL_METER=0 \
  OTLP_ENDPOINT=http://localhost:4317 \
  OTLP_METRICS_PUSH_INTERVAL_SECS=10 \
  cargo run -- --pg
```

## Stop

```bash
docker compose -f tools/observability/docker-compose.yml down
```
