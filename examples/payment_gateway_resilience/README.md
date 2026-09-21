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

For HTTP hosting, add `--features web-host` before Cargo's `--` separator and
`--config examples/payment_gateway_resilience/obzenflow.server.toml` after it.

Source: [flow](flow.rs) and [gateway effects](gateway.rs).
See the [examples index](../README.md) for published tutorials.
