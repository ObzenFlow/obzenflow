<!--
SPDX-License-Identifier: MIT OR Apache-2.0
SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
https://obzenflow.dev
-->

# Payment Gateway Resilience

A durable-execution tutorial built around one realistic stage: a call to an
unreliable payment gateway. It teaches two ideas, in order.

1. **The gateway call is an effect.** Instead of calling the gateway inline, the
   stage returns the call as data. The runtime executes it once, records the
   result in the journal, and on replay returns that recorded result without
   calling the gateway again. This is what makes a recorded run reproducible.
2. **Resilience is a second layer.** A circuit breaker watches the live gateway
   and, once it looks unhealthy, short-circuits to a typed fallback instead of
   hammering it. Local validation errors travel a separate error rail and never
   reach the gateway at all.

The gateway here is simulated so the behaviour is deterministic, but the shape is
the real one: a real implementation would issue an HTTP request inside the
effect's `execute`.

## The flow

```text
payments            validation              gateway                 summary
(source)     ->     (transform)     ->      (effectful transform)   ->   (sink)
scripted           local checks,            AuthorizePayment effect      counts and
commands           deterministic            + circuit breaker            narrates
```

The source scripts three phases so you can correlate behaviour with logs and
metrics: a healthy **warmup**, an **outage** where every gateway call fails, and
a **recovery** that also injects a couple of locally invalid commands.

## 1. The gateway call as an effect

`gateway.rs` defines the call as an `Effect`:

```rust
impl Effect for AuthorizePayment {
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type Output = GatewayAuthorization;

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        // A real gateway call would go here. The runtime owns when this runs.
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> { /* dedupe key */ }
}
```

The stage just performs it and folds the recorded outcome into a typed output:

```rust
let authorization = fx.perform(AuthorizePayment { validated }).await?;
```

Because the effect is declared (`effects: [AuthorizePayment]` on the stage), the
runtime validates the idempotency key before any I/O and journals the outcome
after it. A non-idempotent charge therefore carries a key the gateway can dedupe
on, enforced at build and before execution rather than by hope.

## 2. Deterministic replay

Run the flow once. The gateway is called for each healthy payment, and every
outcome is recorded under `target/payment-gateway-logs`.

```sh
cargo run -p obzenflow --example payment_gateway_resilience
```

Now replay that exact run from its journal:

```sh
# pick the most recent run id
RUN=$(ls -dt target/payment-gateway-logs/flows/*/ | head -1)
cargo run -p obzenflow --example payment_gateway_resilience -- --replay-from "$RUN"
```

On replay the gateway effect is **not executed**. The runtime returns each
recorded authorization, so the summary is reconstructed with zero gateway calls
and the same outcomes. That is the durable-execution property: a recorded run
replays exactly, without re-firing its side effects.

## 3. Resilience as the second layer

While the live run is in progress, the gateway stage also carries a circuit
breaker (`flow.rs`). During the scripted outage the breaker observes the failing
effect boundary and opens. While open it emits a typed degraded authorization
(the fallback) instead of calling the gateway, with a single half-open probe to
test recovery.

What to watch as it runs:

- `obzenflow_errors_total` rises for locally invalid payments (bad card, zero
  amount). These are deterministic validation failures, so they stay on the
  pure transform side and never reach the gateway.
- `obzenflow_circuit_breaker_*{stage="gateway"}` shows the breaker opening during
  the outage, and the sink reports `Authorized (degraded)` for the fallbacks.

On replay, none of this resilience machinery runs: replay performs no live
effect, so the breaker and the rate limiter stay quiet and only the recorded
outcomes drive the result.

## Files

| File | What it holds |
|------|---------------|
| `domain.rs`   | The payment types and the scripted `TrafficPhase`. |
| `gateway.rs`  | Local validation and the gateway call as an effect. |
| `sources.rs`  | The scripted, deterministic source. |
| `fixtures.rs` | The scripted command sequence. |
| `sinks.rs`    | The narrating summary sink. |
| `flow.rs`     | The flow wiring and the circuit-breaker configuration. |
| `main.rs`     | The entry point and CLI banner. |
