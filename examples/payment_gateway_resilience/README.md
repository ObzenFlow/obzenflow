<!--
SPDX-License-Identifier: MIT OR Apache-2.0
SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
https://obzenflow.dev
-->

# Payment gateway resilience

Process orders through an unreliable payment gateway with retries, circuit
breaking, rate limiting, and replayable outcomes.

Run from the repository root. The gateway and orders are simulated; no external
service or extra Cargo feature is required.

```sh
cargo run -p obzenflow --example payment_gateway_resilience
```

The console shows paid, cancelled, and manual-review outcomes. To replay without
calling the gateway, use the archive path printed by the live run:

```sh
cargo run -p obzenflow --example payment_gateway_resilience -- \
  --replay-from <archive> --verify
```

Inspect the same run, including framework evidence and sink lifecycle audits:

```sh
cargo run -p obzenflow --features cli --bin obzenflow -- \
  show <archive> --jsonl --include-runtime
```

Each record carries a semantic event name and a separate positive
`payload_schema_version`. `payment.authorized` is an application fact;
`effect.execution_rejected` records a refusal before execution, while
`effect.execution_failed` records a returned failure without claiming that the
gateway did nothing. The handler can separately author
`payment.authorization_unavailable`.

`delivery.succeeded` identifies its exact committed input in `payload.subject`.
Console success means the process stream accepted and flushed the output.
`sink.flush_succeeded` and `sink.drain_succeeded` describe lifecycle operations
and do not settle inputs. Replay suppresses gateway calls; sinks can emit their
output again. Journal schema 14 requires newly recorded archives.

Framework occurrences identify their author and what that author observed:

| Earlier name | Current name and meaning |
| --- | --- |
| `lifecycle.stage.running` | `supervisor.stage.validate_order.ready`: the named stage reports readiness. |
| `system.pipeline.all_stages_completed` | `supervisor.runtime.pipeline_supervisor.all_stages_completed`: the pipeline observed its required child results. |
| `lifecycle.middleware.circuit_breaker` | `runtime.circuit_breaker.opened`, `runtime.retry.scheduled`, or another typed occurrence. A finished resilience evaluation can include zero attempts. |
| `execution.replay.lifecycle` | `runtime.replay.started`, `runtime.replay.completed`, or `runtime.replay.live_resumed`: the recorded replay occurrence. |
| `control.source_contract` | `runtime.source.contract_declared`: the source's expected production contract; its expected count can be unknown. |
| `control.consumption_final`, source-authored | `runtime.source.production_finalized`: the source's production frontier and end kind. |
| `control.consumption_final`, subscriber-authored | `runtime.subscription.consumption_finalized`: reads, optional receipt frontier, selection and effective policy for one subscription. |
| `execution.contract.result` | `runtime.contract.verification_pending`: verification is incomplete; `phase` distinguishes progress from final evaluation. |
| `execution.contract.pass` | `runtime.contract.continuation_allowed`: policy permits continuation, including configured warnings. The verification finding remains separate. |
| `control.eof` | `runtime.stream.eof_declared`: an upstream declaration, preserving natural, poison or truncated ending. |
| `system.metrics.drained` | `supervisor.runtime.metrics_aggregator.final_snapshot_published`: the aggregator reports reader shutdown and publication of its available current buffer. |

A pipeline observation trusts the child reports it received. These occurrences do
not require stages to agree, and a final marker does not certify viewer coverage.
In the JSONL view, `payload.result` retains the evaluator's evidence and reasons.
Missing producer counts remain unknown; subscriber reads cannot supply them.
Receipt reconciliation can pass while its evidence records failed deliveries.
Each source's production report has no fabricated consumption or success fields.

Use the failed-delivery, contract-policy and interrupted replay tests for branches
absent from a particular live run. Runtime row totals can vary with scheduling;
equal payloads or clocks do not identify duplicate semantic occurrences.

For HTTP hosting, add `--features web-host` before Cargo's `--` separator and
`--config examples/payment_gateway_resilience/obzenflow.server.toml` after it.

Source: [flow](flow.rs) and [gateway effects](gateway.rs).
See the [examples index](../README.md) for published tutorials.
