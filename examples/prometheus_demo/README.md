# Metrics reporting

This example demonstrates the framework’s fixed Prometheus reporting format with explicit opt-in. Ordinary ObzenFlow launches
start neither metrics reporting nor an HTTP host, including when those capabilities are compiled.

Build one executable for reporting enabled and disabled:

```sh
cargo build -p obzenflow --example prometheus_demo --features prometheus,web-host
```

Run with `PROMETHEUS_EVENT_COUNT=100` for a deterministic, small acceptance run:

| Configuration | Behaviour |
| --- | --- |
| `obzenflow.prometheus.toml` | Hosts `/metrics`, waits for Play, then exits after completion |
| `obzenflow.disabled.toml` | Runs immediately with no reporting collection or output |

```sh
PROMETHEUS_EVENT_COUNT=100 target/debug/examples/prometheus_demo \
  --config examples/prometheus_demo/obzenflow.disabled.toml
```

Start the Prometheus variant:

```sh
PROMETHEUS_EVENT_COUNT=100 target/debug/examples/prometheus_demo \
  --config examples/prometheus_demo/obzenflow.prometheus.toml
```

In another terminal, inspect the response and start the waiting flow:

```sh
curl -i http://127.0.0.1:9090/metrics
curl -X POST http://127.0.0.1:9090/api/flow/control \
  -H 'Content-Type: application/json' -d '{"action":"play"}'
```

Replay each printed archive through the same executable and configuration by adding
`--replay-from <archive> --verify`. Prometheus replay also waits for Play. Verification compares
durable output, independently of metrics reporting output, and must report zero differences.

Terminal lifecycle snapshots remain journaled when reporting is disabled. Studio receives final
In/Out/Errors and duration through lifecycle SSE; detailed measurements come from `/metrics`.
Set `[metrics] enabled = true` or `false`; there is no provider selector. The retired
`metrics.exporter` and `OBZENFLOW_METRICS_EXPORTER` settings are rejected, including Prometheus,
noop, and console values. Console metrics reporting is removed. The `console` Cargo feature
still enables Tokio console instrumentation independently.

The backend `studio` capability supplies `prometheus` and `web-host`. Its existing connection
configuration (`obzenflow.studio.toml`) opts in through `studio.enabled = true`; omitted host and
metrics reporting fields receive the required defaults. No UI changes are needed for this backend refactor.
