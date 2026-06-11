<!--
SPDX-License-Identifier: MIT OR Apache-2.0
SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
https://obzenflow.dev
-->

# Payment Gateway Resilience

A durable-execution tutorial built around one realistic stage: authorizing a
payment for a customer order through an unreliable gateway. It teaches four
ideas, in order.

1. **The flow reacts to upstream events.** `CustomerOrderPlaced` is a fact that
   already happened outside this flow. The sources journal those events on a
   live run, and replay reads the archived source events instead of polling the
   sources again.
2. **The gateway authorization is an effect command.** Instead of calling the
   gateway inline, the stage returns `AuthorizePayment` as data. The runtime
   executes it once, records the result in the journal, and on replay returns
   that recorded result without calling the gateway again.
3. **Fan-in is deterministic where effects demand it.** Two order channels
   merge at `validate_order`, and because the effectful `authorize_payment`
   sits below that fan-in, the runtime orders the merge deterministically
   (FLOWIP-095d). Live runs with different arrival timing and replays all
   consume orders in the same merged order.
4. **Resilience is a second layer.** A circuit breaker watches the live gateway
   and, once it looks unhealthy, short-circuits to a typed unavailable outcome
   instead of hammering it.

The gateway here is simulated so the behaviour is deterministic, but the shape is
the real one: a real implementation would issue an HTTP request inside the
effect's `execute`.

## The Flow

```text
web_orders ----+ canonical merge
store_orders --+--> validate_order -- ValidatedOrder --> authorize_payment -- PaymentAuthorized --> paid_orders
                      |                                   |              \-- PaymentAuthorizationUnavailable --> manual_review
                      |                                   |
                      |                                   AuthorizePayment effect + circuit breaker
                      |                                   |
                      \-- OrderCancelled -----------------+-- OrderCancelled --> cancelled_orders
```

`validate_order` classifies each order exactly once and declares a multi-type
output contract: `CustomerOrderPlaced -> { ValidatedOrder, InvalidOrder,
OrderCancelled }`. `InvalidOrder` and `PaymentDeclined` are journal-recorded
provenance facts with no sink of their own; their lifecycle consequence,
`OrderCancelled`, converges from both producers on one cancelled-orders
delivery. The journal is the record, sinks are external deliveries.

The sources script three phases so you can correlate behaviour with logs and
metrics: a healthy **warmup**, an **outage** where every gateway call fails, and
a **recovery** that mixes local validation failures, material gateway declines,
and successful authorizations. The two channels are deliberately different
lengths (13 web, 12 store), so the earlier-ending channel's merged EOF point is
a real thing for replay to reproduce.

## Deterministic Fan-In (FLOWIP-095d)

Before FLOWIP-095d, this topology would not build. An effectful stage below a
fan-in was rejected at flow build time:

```text
Effectful stage 'authorize_payment' is downstream of nondeterministic fan-in. ...
```

The rejection existed because effect replay suppression keys on each input's
delivered position, and an availability-driven merge makes those positions
depend on arrival timing. FLOWIP-095d removes the free choice instead of
recording it: the flow build marks every fan-in above an effectful stage, and
those stages consume their inputs through a canonical deterministic merge (one
held head per input, wait while any non-exhausted input is quiet, causal order
between heads, then a per-input-ordinal/stage-name tiebreak). Delivery order
becomes a pure function of the two recorded streams.

See it hold under arrival-timing chaos. Run the flow twice with different
per-source jitter, then compare the order journals at `validate_order`:

```sh
PAYMENT_DEMO_SOURCE_JITTER_MS=0  cargo run -p obzenflow --example payment_gateway_resilience
PAYMENT_DEMO_SOURCE_JITTER_MS=40 cargo run -p obzenflow --example payment_gateway_resilience
# In each printed run directory, the validate_order journal lists order ids in
# the SAME merged sequence, even though wall-clock arrival differed.
```

The honesty note: the canonical merge waits on a quiet input (that is what
removes arrival timing from the function), so an ordered live fan-in advances
at a rate coupled to its slowest input. Sealed inputs never wait, which is why
replay always runs at disk speed. The `cancelled_orders` sink fan-in carries
the contrast: it is delivery-only with no effectful descendant, so it keeps
availability-driven scheduling and pays no coupling cost.

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

The stage performs the effect through the guarded wrapper (FLOWIP-120h), so the
circuit breaker's branches are explicit in the type rather than smuggled as a
variant of the gateway's own decision. The recorded outcome group is the branch
fact itself, reconstructed by event type on replay:

```rust
let outcome = fx
    .perform(AuthorizePayment { order }.guarded::<GatewayPaymentFallback, GatewayPaymentRejected>())
    .await;

match outcome {
    Ok(CircuitBreakerOutcome::Primary(decision)) => match decision {
        GatewayPaymentDecision::Authorized { .. } => fx.emit(PaymentAuthorized { ... }).await?,
        GatewayPaymentDecision::Declined { .. } => {
            // Fact first, consequence second: the decline is the provenance,
            // the derived cancellation is the order's fate.
            fx.emit(PaymentDeclined { ... }).await?;
            fx.emit(OrderCancelled { ... }).await?
        }
    },
    Ok(CircuitBreakerOutcome::Fallback(fallback)) => {
        // Breaker open: the gateway was never called, no decision exists.
        fx.emit(PaymentAuthorizationUnavailable { ... }).await?
    }
    Ok(CircuitBreakerOutcome::Rejected(rejected)) => {
        fx.emit(PaymentAuthorizationUnavailable { ... }).await?
    }
    Err(err) => {
        // Genuine gateway failure, recorded under the effect cursor.
        fx.emit(PaymentAuthorizationUnavailable { ... }).await?
    }
}
```

Because the effect is declared (`effects: [AuthorizePayment]` on the stage), the
runtime validates the idempotency key before any I/O and journals the outcome
after it. A non-idempotent charge therefore carries a key the gateway can dedupe
on, enforced at build and before execution rather than by hope.

## 3. Facts, Consequences, and Deliveries

This example deliberately keeps three unhappy paths distinct in the journal
while converging two of them into one delivery.

Local validation failures are facts about the order before any gateway I/O. A
zero amount or structurally invalid payment method records an `InvalidOrder`
fact (the provenance) and derives an `OrderCancelled` fact (the consequence):

```text
CustomerOrderPlaced -> validate_order -> InvalidOrder + OrderCancelled
```

Material gateway declines are facts returned by the payment gateway after a
valid authorization attempt. Insufficient funds or billing-address mismatch
records a `PaymentDeclined` fact and derives the same `OrderCancelled`
consequence through `fx.emit`:

```text
ValidatedOrder -> authorize_payment -> PaymentDeclined + OrderCancelled
```

Both cancellations land on the one `cancelled_orders` delivery, each carrying
its reason, while the specific facts stay in the journal for anyone asking why.

Infrastructure unavailability is neither an invalid order nor a gateway
decline, and it deliberately does not cancel: no payment decision was reached,
so manufacturing an order fate out of a gateway outage would record a fake
outcome. Gateway timeouts and breaker-open fallbacks become
`PaymentAuthorizationUnavailable` and go to manual review, while the circuit
breaker still classifies those unavailable outcomes as dependency failures:

```text
ValidatedOrder -> authorize_payment -> manual_review
```

That is the preferred shape for business errors in ObzenFlow. Do not turn every
meaningful unhappy path into a framework processing error just because it is not
the success case. Lean toward typed events first. Shipping subscribes to paid
orders, customer notification subscribes to cancelled orders wherever the
cancellation originated, and an operations workflow subscribes to unavailable
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
journal and the recorded gateway decisions from the effect history, then the
handler re-emits the same named payment facts and derived cancellations.
Downstream paid, cancelled, and unavailable deliveries are reconstructed with
the same outcomes. That is the durable-execution property: a recorded run
replays exactly, without re-pulling inputs or re-firing side effects.

## 5. Resilience as the Second Layer

While the live run is in progress, the gateway stage also carries a circuit
breaker, declared in the `output_middleware:` lane (`flow.rs`) so its branch
fact types are validated as arrow members at build time. During the scripted
outage the breaker observes the failing effect boundary and opens. While open
it synthesizes the named `GatewayPaymentFallback` branch fact instead of
calling the gateway, with a single half-open probe to test recovery. The
handler maps that branch to `PaymentAuthorizationUnavailable` for manual
review, and the journal records the group with a `middleware_synthesized`
origin marker, so breaker activity is auditable without reason-string
inspection.

What to watch as it runs:

- Cancelled orders show up in the `cancelled_orders` sink as `OrderCancelled`
  events, each carrying its reason: locally invalid orders never call the
  gateway, and gateway declines are material remote payment decisions. The
  underlying `InvalidOrder` and `PaymentDeclined` facts are in the stage
  journals, not in any sink.
- `obzenflow_circuit_breaker_*{stage="authorize_payment"}` shows the breaker
  opening during the outage. The `manual_review` sink receives
  authorization-unavailable events for failed gateway calls and breaker
  fallbacks; those orders are not cancelled, because no decision was reached.

On replay, none of this resilience machinery runs: replay performs no live
effect, so the breaker and the rate limiter stay quiet and only the recorded
outcomes drive the result.

## What This Example Deliberately Leaves Out

This tutorial owns the authorization leg of the card lifecycle:
`AuthorizePayment` producing `PaymentAuthorized` is an authorization hold, the
first half of authorize-then-capture. Capture after fulfilment is deliberately
absent, and so is customer-initiated cancellation, which can only be processed
honestly against order state (a cancel request must be checked against
picking, packing, shipped). Both belong to the checkout saga capstone
(FLOWIP-095h), where the order is a state machine. The piggy bank example is
likewise untouched here: it is the canonical resume scenario (FLOWIP-120n) and
has its own published tutorial.

## Files

| File | What it holds |
|------|---------------|
| `domain.rs`   | The payment events, gateway decisions, cancellation lifecycle facts, and scripted `TrafficPhase`. |
| `validation.rs` | One multi-type validation stage that classifies each order exactly once. |
| `gateway.rs`  | Gateway authorization as a replay-suppressed effect, deriving cancellations from declines. |
| `fixtures.rs` | The scripted upstream order-event sequence. |
| `sinks.rs`    | Typed deliveries for paid, cancelled, and unavailable outcomes. |
| `flow.rs`     | The flow wiring and the circuit-breaker configuration. |
| `main.rs`     | The entry point and CLI banner. |
