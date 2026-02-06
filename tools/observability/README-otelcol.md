# Local OTLP Collector + VictoriaMetrics

This spins up VictoriaMetrics + OpenTelemetry Collector + Grafana for local metrics.

## Start

```bash
docker compose -f tools/observability/docker-compose.vm-otelcol.yml up -d
```

## Open UI

VictoriaMetrics UI:
`http://localhost:8428/vmui/`

## Stop

```bash
docker compose -f tools/observability/docker-compose.vm-otelcol.yml down
```
