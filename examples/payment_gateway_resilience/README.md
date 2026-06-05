<!--
SPDX-License-Identifier: MIT OR Apache-2.0
SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
https://obzenflow.dev
-->

# Payment Gateway Resilience

A durable-execution tutorial built around one realistic stage: authorizing a
payment for a customer order through an unreliable gateway. It teaches three
ideas, in order.

1. **The flow reacts to upstream events.** `CustomerOrderPlaced` is a fact that
   already happened outside this flow. The source journals those events on a
   live run, and replay reads the archived source events instead of polling the
   source again.
2. **The gateway authorization is an effect command.** Instead of calling the
   gateway inline, the stage returns `AuthorizePayment` as data. The runtime
   executes it once, records the result in the journal, and on replay returns
   that recorded result without calling the gateway again.
3. **Resilience is a second layer.** A circuit breaker watches the live gateway
   and, once it looks unhealthy, short-circuits to a typed unavailable outcome
   instead of hammering it. Local validation failures and remote gateway
   declines travel separate business channels.

The gateway here is simulated so the behaviour is deterministic, but the shape is
the real one: a real implementation would issue an HTTP request inside the
effect's `execute`.

## The Flow

```text
orders
(source)
scripted upstream events
  |\
  | \-> invalid_orders -> invalid_order_events
  |
  \--> valid_orders -> gateway -> authorized_payments -> paid_orders
                    |          \-> gateway_declines -> declined_payments
                    |          \-> authorization_unavailable -> manual_review
                    |
                    AuthorizePayment effect
                    + circuit breaker
```

The source scripts three phases so you can correlate behaviour with logs and
metrics: a healthy **warmup**, an **outage** where every gateway call fails, and
a **recovery** that mixes local validation failures, material gateway declines,
and successful authorizations.

## 1. Upstream Events Enter Through the Source

`CustomerOrderPlaced` is the upstream business event that triggers payment work
in this flow. In a real deployment it would usually come from HTTP ingestion, a
Kafka topic, a database outbox, or another flow. In this tutorial it comes from a
scripted source so the logs, metrics, circuit-breaker state, and replay are easy
to compare.

The gateway command is `AuthorizePayment`, and that command is represented as an
`Effect`. The input is not a command; it is a recorded fact the flow reacts to.

On replay, ObzenFlow does not poll the source again. The runtime reads the
archived source journal and injects the recorded `CustomerOrderPlaced` events
with replay provenance, so the upstream feed is not re-run.

## 2. The Gateway Command as an Effect

`gateway.rs` defines the outbound gateway command as an `Effect`:

```rust
impl Effect for AuthorizePayment {
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type Output = GatewayPaymentDecision;

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        // A real gateway call would go here. The runtime owns when this runs.
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> { /* dedupe key */ }
}
```

The stage performs the effect and folds the recorded gateway decision into a
typed intermediate outcome:

```rust
let decision = fx.perform(AuthorizePayment { order }).await?;
```

Because the effect is declared (`effects: [AuthorizePayment]` on the stage), the
runtime validates the idempotency key before any I/O and journals the outcome
after it. A non-idempotent charge therefore carries a key the gateway can dedupe
on, enforced at build and before execution rather than by hope.

## 3. Business Outcomes Use Dedicated Channels

This example deliberately keeps three unhappy paths separate.

Local validation failures are facts about the order before any gateway I/O. A
zero amount or structurally invalid payment method becomes an `InvalidOrder`:

```text
CustomerOrderPlaced -> invalid_orders -> invalid_order_events
```

Material gateway declines are facts returned by the payment gateway after a
valid authorization attempt. Insufficient funds or billing-address mismatch
becomes a `PaymentDeclined`:

```text
ValidatedOrder -> gateway -> gateway_declines -> declined_payments
```

Infrastructure unavailability is neither an invalid order nor a gateway decline.
Gateway timeouts and breaker-open fallbacks become
`PaymentAuthorizationUnavailable`, while the circuit breaker still classifies
those unavailable outcomes as dependency failures:

```text
ValidatedOrder -> gateway -> authorization_unavailable -> manual_review
```

That is the preferred shape for business errors in ObzenFlow. Do not turn every
meaningful unhappy path into a framework processing error just because it is not
the success case. Lean toward typed events and dedicated channels first. Shipping
can subscribe to paid orders, a website can subscribe to invalid orders and
gateway declines, and an operations workflow can subscribe to unavailable
authorizations.

Framework errors are still useful for bugs, deserialization failures, broken
handlers, exhausted infrastructure that cannot be represented meaningfully, and
other cases where the flow itself could not correctly process the work. Domain
outcomes should usually be modeled as typed events.

## 4. Deterministic Replay

Run the flow once. The gateway is called for each locally valid payment while the
breaker permits the call, and every outcome is recorded under
`target/payment-gateway-logs`.

```sh
cargo run -p obzenflow --example payment_gateway_resilience
```

Now replay that exact run from its journal:

```sh
# pick the most recent run id
RUN=$(ls -dt target/payment-gateway-logs/flows/*/ | head -1)
cargo run -p obzenflow --example payment_gateway_resilience -- --replay-from "$RUN"
```

On replay the source is **not polled** and the gateway effect is **not
executed**. The runtime returns the recorded upstream events from the source
journal and the recorded gateway decisions from the effect history, so downstream
paid, invalid, declined, and unavailable outputs are reconstructed with the same
outcomes. That is the durable-execution property: a recorded run replays exactly,
without re-pulling inputs or re-firing side effects.

## 5. Resilience as the Second Layer

While the live run is in progress, the gateway stage also carries a circuit
breaker (`flow.rs`). During the scripted outage the breaker observes the failing
effect boundary and opens. While open it emits a typed
`PaymentAuthorizationUnavailable` fallback instead of calling the gateway, with a
single half-open probe to test recovery.

What to watch as it runs:

- Invalid orders show up in the `invalid_order_events` sink as `InvalidOrder`
  events. They do not call the gateway and they are not modeled as framework
  processing errors.
- Gateway declines show up in the `declined_payments` sink as `PaymentDeclined`
  events. These are material remote payment decisions, not local validation.
- `obzenflow_circuit_breaker_*{stage="gateway"}` shows the breaker opening during
  the outage. The `manual_review` sink receives authorization-unavailable events
  for failed gateway calls and breaker fallbacks.

On replay, none of this resilience machinery runs: replay performs no live
effect, so the breaker and the rate limiter stay quiet and only the recorded
outcomes drive the result.

## Files

| File | What it holds |
|------|---------------|
| `domain.rs`   | The payment events, gateway decisions, and scripted `TrafficPhase`. |
| `validation.rs` | Local validation as typed business branching. |
| `gateway.rs`  | Gateway authorization as a replay-suppressed effect. |
| `fixtures.rs` | The scripted upstream order-event sequence. |
| `sinks.rs`    | Typed sink actions for paid, invalid, declined, and unavailable outcomes. |
| `flow.rs`     | The flow wiring and the circuit-breaker configuration. |
| `main.rs`     | The entry point and CLI banner. |
