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
output again. Journal schema 13 requires newly recorded archives.

For HTTP hosting, add `--features web-host` before Cargo's `--` separator and
`--config examples/payment_gateway_resilience/obzenflow.server.toml` after it.

Source: [flow](flow.rs) and [gateway effects](gateway.rs).
See the [examples index](../README.md) for published tutorials.
