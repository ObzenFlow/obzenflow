# Metrics reporting

Observe Prometheus metrics while a flow exercises circuit breaking and
backpressure.

The flow processes inputs, counts successful results and prints that recorded
count. The summary has the same meaning during replay, even when the configured
input count changes. The framework supplies source and error metrics automatically.

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

For Tokio Console diagnostics, build with `tokio-console` and opt in explicitly:

```sh
PROMETHEUS_TOKIO_CONSOLE=1 cargo run -p obzenflow --example prometheus_demo \
  --features prometheus,web-host,tokio-console -- \
  --config examples/prometheus_demo/obzenflow.prometheus.toml --startup-mode auto
```

This starts the flow automatically. Connect the Tokio Console client to
`http://127.0.0.1:6669`. The repository's
Cargo configuration already enables `tokio_unstable`, which diagnostics require.
`PROMETHEUS_TOKIO_CONSOLE` unset or `0` leaves the subscriber disabled, including
in the same Console-capable binary. Other values, or `1` without the feature,
fail explicitly. Console poll durations are elapsed time within polls; they do
not by themselves establish CPU time or the cause of pending waits.
Managed Console rejects `TOKIO_CONSOLE_RECORD_PATH`: the upstream recorder's
separate thread cannot join the application's shutdown lifecycle.

For the lightweight source/transform cycle breakdown, use
`RUST_LOG=info,obzenflow::supervisor_timing=debug,obzenflow::performance=off`.
This records exclusive read, handler, preparation, publication, credit-wait,
acknowledgement, control, idle and residual time without collecting deep spans.
The same native report below verifies and presents the cycle budget.

For nested subscription and disk read/write drill-down, enable both diagnostic
targets. This also works with Console off in the same build:

```sh
PROMETHEUS_RATE_LIMIT=0 PROMETHEUS_TOKIO_CONSOLE=0 PROMETHEUS_EVENT_COUNT=1000 \
  RUST_LOG=info,obzenflow::supervisor_timing=debug,obzenflow::performance=debug \
  cargo run --locked -p obzenflow --example prometheus_demo \
  --features prometheus,web-host,tokio-console -- \
  --config examples/prometheus_demo/obzenflow.prometheus.toml --startup-mode auto
```

Each supervisor reports disjoint elapsed phases per FSM state. The final
`performance_capture` log contains bounded aggregates of nested span paths,
including separate reads, appends, accounting/codec work, locks and blocking
dispatch/worker intervals. Source and transform dispatch children distinguish
upstream polling, handler invocation, output preparation/publication, downstream
credit waiting and upstream acknowledgement. Codec children separate provenance,
observability and payload JSON from full-record size accounting, metadata lookups
and frame assembly. Keep the full log. With the printed live archive path,
the existing native measurement produces JSON and Markdown after checking outcomes:

```sh
PROMETHEUS_MEASUREMENT_ARCHIVE=<archive> PROMETHEUS_MEASUREMENT_INPUTS=1000 \
  PROMETHEUS_MEASUREMENT_RATE_LIMIT=0 \
  PROMETHEUS_MEASUREMENT_LOG=<log> cargo test --locked -p obzenflow \
  --features prometheus,web-host,tokio-console --test prometheus_example_test \
  measure_retained_prometheus_archive -- --ignored --nocapture
```

Incomplete captures, lost spans and nonconserved phase totals fail verification.
`PROMETHEUS_RATE_LIMIT=0` removes the source limiter; unset or `1` retains the
default 1,000/s policy. Other values fail before launch. The matching measurement
flag verifies absence/presence of the limiter in durable configuration evidence;
the breaker and window 64 remain in place. The report also binds every supervisor
writer to the verified archive, so a replay or another run's capture cannot pass
merely because its log mentions the original flow ID.
The runner phases supply a 100% time budget for each supervisor/state. Source and
transform cycles have a separate exclusive budget ending at dispatch return or
cancellation, including retries and draining. Cycle counts differ from business
inputs; input-to-ack latency can span multiple cycles. Reported category shares
divide summed phase time by summed cycle time. Error/cancelled cycles and residual
violations stay visible. Exact accounting does not establish measurement accuracy:
the ±5-percentage-point qualification still requires measured observer uncertainty
and repeated representative captures.

Nested
operations are inclusive; concurrent operations can overlap. Entered time is
wall time in polls/blocks, not CPU time, and buffered file calls do not establish
physical disk service time. Tables report totals, call counts and mean microseconds
per call; a total across 5,000 calls is not one event's latency. Outer runner turns
also differ from completed dispatches and business inputs. Source credit-backoff
dispatches can return before its existing work-count utilisation counter advances.
Compare diagnostics off/on before drawing performance
conclusions. Default logging emits no timing capture.

To run without reporting, select [the disabled configuration](obzenflow.disabled.toml);
it starts immediately. For Prometheus and Grafana setup, see
[the monitoring guide](../../monitoring/README.md).

Source: [flow and handlers](main.rs).
See the [examples index](../README.md) for published tutorials.
