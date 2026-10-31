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
input to an error journal, and exits on completion. Set `event_count = 100`
under `[app]` in a copy of the config for a short run without the simulated
outages. Start-up output lists the settings the run used.

To replay, repeat the run command with `--replay-from <archive> --verify` appended,
using the archive path printed by the live run. Replay also waits for Play;
send the same control request again. Verification compares durable output.

For Tokio Console diagnostics, use the `tokio-console-run` alias and opt in
explicitly. The alias enables the `tokio-console` feature and supplies
`--cfg tokio_unstable`, which diagnostics require, in its own target directory:

```sh
cargo tokio-console-run --example prometheus_demo --features prometheus,web-host -- \
  --config examples/prometheus_demo/obzenflow.prometheus.toml --startup-mode auto --tokio-console
```

This starts the flow automatically. Connect the Tokio Console client to
`http://127.0.0.1:6669`.

To run without reporting, select [the disabled configuration](obzenflow.disabled.toml);
it starts immediately. For Prometheus and Grafana setup, see
[the monitoring guide](../../monitoring/README.md).

Source: [flow and handlers](main.rs).
See the [examples index](../README.md) for published tutorials.
