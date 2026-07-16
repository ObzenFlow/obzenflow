// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The payment-gateway resilience flow.
//!
//! Two upstream order channels, three business deliveries:
//!
//! ```text
//! web_orders ---+ canonical merge
//! store_orders -+-> validate_order -- ValidatedOrder --> authorize_payment -- PaymentAuthorized --> paid-orders sink
//!                     |                                    |             \-- PaymentAuthorizationUnavailable --> manual-review sink
//!                     \-- OrderCancelled --------------+   \-- OrderCancelled --+
//!                                                      \----------------------(fan-in)--> cancelled-orders sink
//! ```
//!
//! The channel merge at `validate_order` is FLOWIP-095d's canonical
//! deterministic merge, enabled automatically because the effectful
//! `authorize_payment` sits below the fan-in. Delivery order is a pure
//! function of the two recorded streams: live runs with different arrival
//! timing and replays all consume orders in the same merged order. The
//! `cancelled_orders` sink fan-in stays availability-driven, since it is
//! delivery-only with no effectful descendant.
//!
//! Validation lives in `validation.rs` as one typed classifier
//! (`CustomerOrderPlaced -> ValidationOutcome`) whose flat output contract is
//! `{ ValidatedOrder, InvalidOrder, OrderCancelled }`;
//! the authorize_payment stage performs the `AuthorizePayment` effect (see
//! `gateway.rs`). On a live run the effect executes once and the runtime
//! journals its result; on a replay the runtime returns that recorded gateway
//! decision without calling the gateway again. The stage then emits the named
//! payment fact that happened, deriving `OrderCancelled` from declines.
//!
//! `InvalidOrder` and `PaymentDeclined` are journal-recorded facts with no
//! dedicated sink: the journal is the record, sinks are external deliveries.
//! The cancelled-orders sink converges cancellations from both producers.
//!
//! The circuit breaker on the authorize_payment stage is the second,
//! independent layer: it watches the live effect boundary and, once the
//! dependency looks unhealthy, fails fast with a recorded rejection instead
//! of hammering it. Unavailability deliberately does not cancel; no decision
//! was reached, so those orders go to manual review. See `README.md`.

use super::console;
use super::deliveries::ShippingHandoff;
use super::domain::{
    CustomerOrderPlaced, InvalidOrder, OrderCancelled, PaymentAuthorizationUnavailable,
    PaymentAuthorized, PaymentDeclined, ValidatedOrder,
};
use super::fixtures;
use super::gateway::{self, AuthorizePayment, GatewayTransform};
use super::validation;
use obzenflow::typed::sources as typed_sources;
use obzenflow_adapters::middleware::circuit_breaker::OpenPolicy;
use obzenflow_adapters::middleware::observability::{indicator, log, IndicatorKind};
use obzenflow_adapters::middleware::{failure_rate, CircuitBreaker, RateLimiterBuilder, Retry};
use obzenflow_dsl::{effectful_transform, flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use std::time::Duration;

const SOURCE_RATE_LIMIT_EVENTS_PER_SECOND: f64 = 20.0;
const SOURCE_RATE_LIMIT_BURST: f64 = 1.0;
const GATEWAY_CALLS_PER_SECOND: f64 = 1.0;

/// Where the demo binary journals its runs. The acceptance rig in `proof.rs`
/// shares it so `--replay-from` paths printed by one entry point work for both.
pub(super) const DEMO_JOURNAL_ROOT: &str = "target/payment-gateway-logs";

/// Optional per-source pacing jitter (FLOWIP-095d demo knob).
///
/// `PAYMENT_DEMO_SOURCE_JITTER_MS=<max>` delays each order by a deterministic
/// pseudo-random duration derived from (channel, index). Two live runs with
/// different jitter settings arrive in different wall-clock orders, and the
/// canonical merge at `validate_order` still consumes them in the same merged
/// order, asserted from the journals rather than from timing.
fn demo_jitter(channel: &str, index: usize) {
    // The FLOWIP-095d demo knob varies arrival timing only, never stream
    // content, which is precisely what the canonical merge is insensitive to.
    let max_ms: u64 = std::env::var("PAYMENT_DEMO_SOURCE_JITTER_MS") // allow-replay-ambient: timing-only demo pacing knob, stream content unaffected
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    if max_ms == 0 {
        return;
    }
    // FNV-1a over (channel, index): reproducible for a given setting, varied
    // across channels and indices.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in channel.bytes().chain(index.to_le_bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    std::thread::sleep(std::time::Duration::from_millis(hash % (max_ms + 1)));
}

/// Build the tutorial flow: the scripted three-phase order channels, the real
/// gateway transform, and the breaker with no retry section.
///
/// Gentle source and gateway-effect rate limits keep logs and metrics readable,
/// so you can watch source-boundary and effect-boundary policy metrics change
/// over time.
pub fn build_flow() -> obzenflow_dsl::FlowDefinition {
    assemble_flow(
        fixtures::scripted_web_orders(),
        fixtures::scripted_store_orders(),
        GatewayTransform::default(),
        None,
        GATEWAY_CALLS_PER_SECOND,
        std::path::PathBuf::from(DEMO_JOURNAL_ROOT),
    )
}

/// Assemble the flow from its variable ingredients. The tutorial calls it with
/// the defaults above; the acceptance rig in `proof.rs` swaps in its scripted
/// order, instrumented gateway, and retry section without touching the
/// topology.
pub fn assemble_flow(
    scripted_web_orders: Vec<CustomerOrderPlaced>,
    scripted_store_orders: Vec<CustomerOrderPlaced>,
    gateway_transform: GatewayTransform,
    gateway_retry: Option<Retry>,
    gateway_calls_per_second: f64,
    journal_root: std::path::PathBuf,
) -> obzenflow_dsl::FlowDefinition {
    // The gateway breaker attaches inline to the effect it guards
    // (FLOWIP-120c H7, `AuthorizePayment with [gateway_breaker]`): one
    // policy instance per protected dependency. Hoisted to a named binding
    // so the `effects:` entry reads as a declaration.
    let gateway_breaker = CircuitBreaker::opens_when(
        failure_rate(0.6)
            .over_last_calls(5)
            .or_slow_calls_over(Duration::from_millis(250), 0.5),
    )
    .cooldown(Duration::from_secs(5))
    .when_open(OpenPolicy::FailFast)
    .when_probe_rejected(OpenPolicy::FailFast)
    .with_failure_classification(gateway::classify_simulated_gateway_unavailability);
    let gateway_breaker = match gateway_retry {
        Some(retry) => gateway_breaker.retry(retry),
        None => gateway_breaker,
    };
    let gateway_breaker = gateway_breaker.build();
    let gateway_limiter = RateLimiterBuilder::new(gateway_calls_per_second).build();

    flow! {
        name: "payment_gateway_resilience_demo",
        journals: disk_journals(journal_root),
        middleware: [],

        stages: {
            // Sources: two scripted order channels across three phases
            // (warmup, outage, recovery), of deliberately unequal length.
            //
            // The flow reacts to these facts. On replay the runtime injects
            // journaled source events instead of polling these sources again.
            // Optional jitter (PAYMENT_DEMO_SOURCE_JITTER_MS) varies arrival
            // timing without changing the merged delivery order, because the
            // canonical merge at validate_order orders by stream content.
            //
            // The source rate limiter is the source-boundary example
            // (FLOWIP-115a). These are local scripted fixtures, so a source
            // circuit breaker would be misleading here; the breaker belongs
            // on external dependencies such as the gateway effect below.
            web_orders = source!(CustomerOrderPlaced => typed_sources::finite_from_fn(move |index| {
                let order = scripted_web_orders.get(index).cloned();
                if order.is_some() {
                    demo_jitter("web", index);
                }
                order
            }), [
                RateLimiterBuilder::new(SOURCE_RATE_LIMIT_EVENTS_PER_SECOND)
                    .with_burst(SOURCE_RATE_LIMIT_BURST)
                    .build()
            ]);
            store_orders = source!(CustomerOrderPlaced => typed_sources::finite_from_fn(move |index| {
                let order = scripted_store_orders.get(index).cloned();
                if order.is_some() {
                    demo_jitter("store", index);
                }
                order
            }), [
                RateLimiterBuilder::new(SOURCE_RATE_LIMIT_EVENTS_PER_SECOND)
                    .with_burst(SOURCE_RATE_LIMIT_BURST)
                    .build()
            ]);

            // Local validation: deterministic checks with no external I/O,
            // classified exactly once by one multi-type stage. This is typed
            // business classification, not exception handling: a valid order
            // becomes `ValidatedOrder`, an invalid order records the
            // `InvalidOrder` fact and its derived `OrderCancelled` consequence.
            // Its typed carrier lowers directly to the declared flat facts;
            // no effect cursor or effect provenance is involved.
            validate_order = transform!(
                CustomerOrderPlaced -> validation::ValidationOutcome,
                outputs: [
                    ValidatedOrder,
                    InvalidOrder,
                    OrderCancelled
                ] => validation::ValidateOrder
            );

            // Payment authorization: the only stage that touches the outside
            // world, expressed as the `AuthorizePayment` effect. The runtime
            // runs it once, records the result, and suppresses it on replay.
            // This is the authorization (hold) leg of authorize-then-capture;
            // capture after fulfilment belongs to the checkout saga capstone.
            //
            // The circuit breaker is the live-run safety layer on top:
            //   - opens when >= 60% of the last 5 gateway calls fail,
            //   - also counts calls slower than 250ms toward opening,
            //   - while open, fails fast: the prevented call surfaces as a
            //     recorded rejection error, never a synthesized fact, with a
            //     single half-open probe.
            // The breaker attaches inline to the effect it guards
            // (FLOWIP-120c H7): one policy instance per protected
            // dependency. A prevented call is recorded under the effect
            // cursor like any other failure, so contracts stay strict with
            // no breaker-aware compensation: every admitted input has a
            // journaled outcome whether the gateway ran or the breaker
            // refused the call, and the handler routes the refusal to
            // manual review.
            authorize_payment = effectful_transform!(
                ValidatedOrder -> {
                    PaymentAuthorized,
                    PaymentDeclined,
                    OrderCancelled,
                    PaymentAuthorizationUnavailable
                } => gateway_transform,
                effects: [AuthorizePayment with [gateway_breaker, gateway_limiter]],
                // Record a per-execution service-level-indicator sample for the
                // authorization operation: the raw wall-clock latency of the live
                // gateway call. This is observe-only evidence; it never changes
                // whether the payment succeeds, retries, or routes. The objective
                // (e.g. "under five seconds"), and aggregation into percentiles and
                // SLOs, are FLOWIP-115l's job, applied at read time over these
                // journalled samples rather than baked into the wide event.
                middleware: [
                    indicator()
                        .operation("payment.authorization")
                        .kind(IndicatorKind::Latency)
                        .indicator("authorization.latency")
                        .tag("dependency", "payment_gateway")
                ]
            );

            // Paid-order sink, tier 3: a typed delivery. `ShippingHandoff`
            // carries its destination identity, duplicate-safety, and
            // behaviour on the type; the receipt's journalled destination is
            // its DELIVERY_TYPE ("shipping.handoff"), and resume needs no
            // operator flag because SAFETY is declared at compile time.
            paid_orders = sink!(PaymentAuthorized => ShippingHandoff::new());

            // Cancelled-order sink, tier 2: a declared closure. The order's
            // fate converges from both producers (local validation failures
            // and gateway declines). `InvalidOrder` and `PaymentDeclined`
            // stay journal-recorded facts with no dedicated sink; this
            // delivery carries the lifecycle consequence wherever it
            // originated. The second closure argument is the per-delivery
            // provenance context (FLOWIP-120i): labelling only, never a
            // reason to skip the write.
            cancelled_orders = sink!(
                OrderCancelled => |cancelled, delivery| {
                    console::record_cancelled_order(cancelled, delivery.provenance());
                },
                delivery: idempotent
            );

            // Unavailable-authorization sink, tier 2 with middleware: failed
            // gateway call or breaker refusal. No payment decision was
            // reached, so the order is not cancelled; it goes to retry or
            // manual review.
            manual_review = sink!(
                PaymentAuthorizationUnavailable => |unavailable, delivery| {
                    console::record_authorization_unavailable(unavailable, delivery.provenance());
                },
                delivery: idempotent,
                middleware: [
                    // Publish journalled operator-handoff evidence for each
                    // unavailable-authorization delivery. Observe-only: it does
                    // not change routing or delivery. The stage data journal is
                    // the source of truth, with a tracing mirror for local
                    // visibility.
                    log().prefix("manual_review")
                ]
            );
        },

        topology: {
            web_orders |> validate_order;
            store_orders |> validate_order;
            validate_order |> authorize_payment;
            authorize_payment |> paid_orders;
            validate_order |> cancelled_orders;
            authorize_payment |> cancelled_orders;
            authorize_payment |> manual_review;
        }
    }
}
