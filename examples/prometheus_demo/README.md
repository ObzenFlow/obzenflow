# Metrics reporting

Observe Prometheus metrics while a flow exercises circuit breaking and
backpressure.

Run from the repository root with port 9090 available. This configuration hosts
`/metrics` and waits for Play:

```sh
cargo run -p obzenflow --example prometheus_demo --features prometheus,web-host -- \
  --config examples/prometheus_demo/obzenflow.prometheus.toml
```

In another terminal, inspect metrics and start the flow:

```sh
curl -i http://127.0.0.1:9090/metrics
curl -X POST http://127.0.0.1:9090/api/flow/control \
  -H 'Content-Type: application/json' -d '{"action":"play"}'
```

The default run processes 100,000 inputs, intentionally routes every hundredth
input to an error journal, and exits on completion. Prefix the run command with
`PROMETHEUS_EVENT_COUNT=100` for a short run without the simulated outages.

To replay, repeat the run command with `--replay-from <archive> --verify` appended,
using the archive path printed by the live run. Replay also waits for Play;
send the same control request again. Verification compares durable output.

To run without reporting, select [the disabled configuration](obzenflow.disabled.toml);
it starts immediately. For Prometheus and Grafana setup, see
[the monitoring guide](../../monitoring/README.md).

Source: [flow and handlers](main.rs).
See the [examples index](../README.md) for published tutorials.
