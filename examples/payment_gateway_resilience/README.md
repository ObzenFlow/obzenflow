<!--
SPDX-License-Identifier: MIT OR Apache-2.0
SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
https://obzenflow.dev
-->

# Payment gateway resilience

Process orders through an unreliable payment gateway with retries, circuit
breaking, rate limiting, and replayable outcomes.

Run from the repository root. The gateway and orders are simulated; no external
service or extra Cargo feature is required. The gateway records every call in
`target/gateway-calls.jsonl`; remove it before each fresh run, never before a resume.

```sh
rm -f target/gateway-calls.jsonl
cargo run -p obzenflow --example payment_gateway_resilience
```

The console shows paid, cancelled, and manual-review outcomes. To replay without
calling the gateway, use the archive path printed by the live run:

```sh
cargo run -p obzenflow --example payment_gateway_resilience -- \
  --replay-from <archive> --verify
```

To try crash recovery, kill the run with `kill -9` after its order feeds
finish, about 0.6 s in, then resume the killed run:

```sh
cargo run -p obzenflow --example payment_gateway_resilience -- \
  --resume-from <archive>
```

Check the gateway's calls against both runs:

```sh
cargo run -p obzenflow --features cli --bin obzenflow -- \
  verify calls --run <archive> --run <resumed-archive> target/gateway-calls.jsonl
```

The feeds are fixed lists, which cannot continue a partial read, so a run
killed before they finish refuses to resume. Replay it instead.

Inspect the same run, including framework evidence and sink lifecycle audits:

```sh
cargo run -p obzenflow --features cli --bin obzenflow -- \
  show <archive> --jsonl --include-runtime
```

For HTTP hosting, add `--features web-host` before Cargo's `--` separator and
`--config examples/payment_gateway_resilience/obzenflow.server.toml` after it.

Source: [flow](flow.rs) and [gateway effects](gateway.rs).
See the [examples index](../README.md) for published tutorials.
