# Monitoring modes

This example explicitly enables its configured monitoring surface. Ordinary ObzenFlow launches
start neither monitoring nor an HTTP host, including when those capabilities are compiled.

Build one executable for all modes:

```sh
cargo build -p obzenflow --example prometheus_demo --features prometheus,web-host
```

Run with `PROMETHEUS_EVENT_COUNT=100` for a deterministic, small acceptance run:

| Configuration | Behaviour |
| --- | --- |
| `obzenflow.prometheus.toml` | Hosts `/metrics`, waits for Play, then exits after completion |
| `obzenflow.console.toml` | Runs immediately, prints summaries, opens no host |
| `obzenflow.disabled.toml` | Runs immediately with no metrics collection or output |
| `obzenflow.noop.toml` | Explicit no-op selection, with no collection or output |

```sh
PROMETHEUS_EVENT_COUNT=100 target/debug/examples/prometheus_demo \
  --config examples/prometheus_demo/obzenflow.console.toml
```

For the Prometheus variant, inspect the response and start the waiting flow:

```sh
curl -i http://127.0.0.1:9090/metrics
curl -X POST http://127.0.0.1:9090/api/flow/control \
  -H 'Content-Type: application/json' -d '{"action":"play"}'
```

Replay each printed archive through the same executable and configuration by adding
`--replay-from <archive> --verify`. Prometheus replay also waits for Play. Verification compares
durable output, independently of monitoring output, and must report zero differences.

Console summaries describe the latest available application and infrastructure observations
with separate timestamps. After Runtime cleanup, one closing attempt shares a deadline of at most
one second with writer teardown, clipped to remaining application shutdown time. A stalled or
broken output can omit the report without changing the flow result. The console transport owns a
system copy process (`/bin/cat` on Unix; PowerShell on Windows) so a blocked stdout write can be
terminated. Process diagnostics use stderr. This is independent of the `console` Cargo feature,
which enables `tokio-console` support.

The backend `studio` capability supplies `prometheus` and `web-host`. Its existing connection
configuration (`obzenflow.studio.toml`) opts in through `studio.enabled = true`; omitted host and
monitoring fields receive the required defaults. No UI changes are needed for this backend refactor.
