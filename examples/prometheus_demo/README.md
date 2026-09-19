# Metrics reporting

This example demonstrates the framework’s fixed Prometheus reporting format with explicit opt-in. Ordinary ObzenFlow launches
start neither metrics reporting nor an HTTP host, including when those capabilities are compiled.

The default 100,000-input run also exercises circuit breaking and backpressure. Source
intake is limited to 1,000 events/second. After every 20,000 inputs, the simulated input
service times out: its breaker opens for five seconds, the first half-open probe fails,
and it opens for another five seconds before recovering. Watch `high_volume_source` in
Studio for `Open → Half-open → Open → Half-open → Closed`. Each opening carries its
five-second cooldown to Studio. Runs of 20,000 inputs or fewer finish before the first
outage; the small acceptance runs below do not exercise breaker recovery.

Backpressure is enabled on all four edges, with a 64-event window and a 30-second stall
timeout. These are demo defaults declared in the flow and can be overridden through
`[runtime.backpressure.flow]`. The outage retries preserve all input IDs; the existing
transform still routes every hundredth input to its error journal. A default run therefore
retains 100,000 inputs and 99,000 successful processed events, with four ten-second
outages in addition to its ordinary processing time.

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
In/Out/Errors and duration through lifecycle SSE. Stage and flow throughput arrive as retained
`throughput_update` bundles on that stream; other detailed metrics still come from `/metrics`.
Hosting Studio updates keeps the shared metrics producer active even when `[metrics] enabled = false`.

For storage-sensitive workloads, optional journal diagnostics can be throttled:

```toml
[runtime.observability]
mode = "periodic"
interval_ms = 250
export_interval_ms = 250
```

The framework default is `mode = "every_record"`; this example's `obzenflow.toml`
explicitly configures both intervals at 250 ms. Each data, error and system journal has its
own allowance. At 250 ms, a journal carries at most four observability packets per
second; slow journals can attach a packet to every record, and idle journals emit
nothing. Payloads, provenance, accounting and
delivery receipts remain complete.

`export_interval_ms` independently controls backend observation publication and each SSE
connection's observation cadence. Its default is 250 ms, and it must be positive. Rates use
the actual monotonic time between successful counter reads. Studio and Prometheus retain
the last complete measurement until a new one arrives, including after completion. Before
two valid samples, throughput is unavailable. A new unchanged counter sample measures zero.
There is no browser smoothing window or age-based replacement.

Prometheus exposes `obzenflow_throughput_events_per_second` with `scope="stage"`,
`scope="flow_input"` or `scope="flow_output"`, and the configured interval as
`obzenflow_observation_export_interval_seconds`. These observations do not replace the
journal-derived totals or establish delivery guarantees.

Use `[runtime.observability.flow]` for flow overrides and
`[runtime.observability.stages.<stage>]` for a stage's `mode` override. The interval
is flow-wide. Environment equivalents are `OBZENFLOW_RUNTIME_OBSERVABILITY_MODE`
and `OBZENFLOW_RUNTIME_OBSERVABILITY_INTERVAL_MS`. The export counterpart is
`OBZENFLOW_RUNTIME_OBSERVABILITY_EXPORT_INTERVAL_MS`; both intervals accept flow overrides
and apply on restart. The export interval is independent of journal `mode`.

Known limitation: after a very large replay, such as one million events, some stage metric
calculations may not yet reflect live-only conditions. FLOWIP-145e owns full replay/live
measurement isolation. Existing replay suppression and the first live throughput baseline
remain in force.

Set `[metrics] enabled = true` or `false`; there is no provider selector. The retired
`metrics.exporter` and `OBZENFLOW_METRICS_EXPORTER` settings are rejected, including Prometheus,
noop, and console values. Console metrics reporting is removed. The `tokio-console` Cargo feature
enables Tokio Console instrumentation independently.

The backend `studio` capability supplies `prometheus` and `web-host`. Its existing connection
configuration (`obzenflow.studio.toml`) opts in through `studio.enabled = true`; omitted host and
metrics reporting fields receive the required defaults.
