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
//! Validation lives in `validation.rs` as one multi-type stage
//! (`CustomerOrderPlaced -> { ValidatedOrder, InvalidOrder, OrderCancelled }`);
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
//! dependency looks unhealthy, short-circuits to a typed unavailable outcome
//! instead of hammering it. Unavailability deliberately does not cancel; no
//! decision was reached, so those orders go to manual review. See `README.md`.

use super::console;
use super::deliveries::ShippingHandoff;
use super::domain::{
    CustomerOrderPlaced, GatewayPaymentFallback, GatewayPaymentRejected, InvalidOrder,
    OrderCancelled, PaymentAuthorizationUnavailable, PaymentAuthorized, PaymentDeclined,
    ValidatedOrder,
};
use super::fixtures;
use super::gateway::{self, AuthorizePayment, GatewayRetryProof, GatewayTransform};
use super::validation;
use obzenflow::typed::sources as typed_sources;
use obzenflow_adapters::middleware::circuit_breaker::{HalfOpenPolicy, OpenPolicy, RetryLimits};
use obzenflow_adapters::middleware::observability::{indicator, log, IndicatorKind};
use obzenflow_adapters::middleware::{CircuitBreakerBuilder, RateLimiterBuilder};
use obzenflow_dsl::{effectful_transform, flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

const SOURCE_RATE_LIMIT_EVENTS_PER_SECOND: f64 = 20.0;
const SOURCE_RATE_LIMIT_BURST: f64 = 1.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryProofProfile {
    Control,
    Treatment,
}

fn retry_proof_profile() -> Option<RetryProofProfile> {
    let profile = std::env::var("PAYMENT_DEMO_RETRY_PROOF"); // allow-replay-ambient: build-time acceptance-test profile only
    match profile {
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("PAYMENT_DEMO_RETRY_PROOF could not be read: {error}"),
        Ok(value) if value == "control" => Some(RetryProofProfile::Control),
        Ok(value) if value == "treatment" => Some(RetryProofProfile::Treatment),
        Ok(value) => panic!(
            "unknown PAYMENT_DEMO_RETRY_PROOF value '{value}'; expected 'control' or 'treatment'"
        ),
    }
}

/// Build the demo flow.
///
/// Gentle source and gateway-effect rate limits keep logs and metrics readable,
/// so you can watch source-boundary and effect-boundary policy metrics change
/// over time.
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

pub fn build_flow() -> obzenflow_dsl::FlowDefinition {
    let retry_proof_profile = retry_proof_profile();
    let replay_requested = std::env::args().any(|argument| argument == "--replay-from");
    let retry_proof =
        retry_proof_profile.map(|_| Arc::new(GatewayRetryProof::new(replay_requested)));
    build_flow_for_profile(
        retry_proof_profile,
        retry_proof,
        std::path::PathBuf::from("target/payment-gateway-logs"),
    )
}

pub fn build_flow_for_profile(
    retry_proof_profile: Option<RetryProofProfile>,
    retry_proof: Option<Arc<GatewayRetryProof>>,
    journal_root: std::path::PathBuf,
) -> obzenflow_dsl::FlowDefinition {
    let (scripted_web_orders, scripted_store_orders) = if retry_proof_profile.is_some() {
        (vec![fixtures::retry_proof_order()], Vec::new())
    } else {
        (
            fixtures::scripted_web_orders(),
            fixtures::scripted_store_orders(),
        )
    };
    let gateway_transform = retry_proof
        .clone()
        .map(GatewayTransform::with_retry_proof)
        .unwrap_or_default();

    // The gateway breaker attaches inline to the effect it guards
    // (FLOWIP-120c H7, `AuthorizePayment with [gateway_breaker]`): one
    // policy instance per protected dependency. Hoisted to a named binding
    // so the `effects:` entry reads as a declaration.
    let mut gateway_breaker = CircuitBreakerBuilder::new(3)
        .cooldown(std::time::Duration::from_secs(5))
        .rate_based_over_last_n_calls(5, 0.6)
        .slow_call(std::time::Duration::from_millis(250), 0.5)
        .with_fallback_fact::<ValidatedOrder, GatewayPaymentFallback, _>(|order| {
            GatewayPaymentFallback {
                order_id: order.order_id.clone(),
                customer_id: order.customer_id.clone(),
                amount_cents: order.amount_cents,
                phase: order.phase.clone(),
                reason: "circuit breaker open".to_string(),
            }
        })
        .with_rejection_fact::<ValidatedOrder, GatewayPaymentRejected, _>(|order, reason| {
            GatewayPaymentRejected {
                order_id: order.order_id.clone(),
                customer_id: order.customer_id.clone(),
                amount_cents: order.amount_cents,
                phase: order.phase.clone(),
                reason: format!("{reason:?}"),
            }
        })
        .with_failure_classifier(gateway::simulated_gateway_unavailability_counts_as_failure)
        .open_policy(OpenPolicy::EmitFallback)
        .half_open_policy(HalfOpenPolicy::new(
            NonZeroU32::new(1).expect("permitted_probes must be non-zero"),
            OpenPolicy::EmitFallback,
        ));
    if matches!(retry_proof_profile, Some(RetryProofProfile::Treatment)) {
        gateway_breaker = gateway_breaker
            .with_retry_fixed(Duration::from_millis(1), 3)
            .with_retry_limits(RetryLimits {
                max_single_delay: Duration::from_millis(10),
                max_attempt_start_window: Duration::from_secs(1),
            });
    }
    let gateway_breaker =
        gateway_breaker.build_typed::<GatewayPaymentFallback, GatewayPaymentRejected>();
    let gateway_limiter = RateLimiterBuilder::new(if retry_proof_profile.is_some() {
        1_000.0
    } else {
        1.0
    })
    .build();

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
            // The empty `effects:` list is explicit: this stage performs no
            // external I/O; the effectful macro surface is what provides
            // multi-type emission today.
            validate_order = effectful_transform!(
                CustomerOrderPlaced -> {
                    ValidatedOrder,
                    InvalidOrder,
                    OrderCancelled
                } => validation::ValidateOrder,
                effects: [],
                middleware: []
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
            //   - while open, synthesizes the named `GatewayPaymentFallback`
            //     branch fact instead of calling the gateway (EmitFallback),
            //     with a single half-open probe.
            // The breaker attaches inline to the effect it guards
            // (FLOWIP-120c H7): one policy instance per protected
            // dependency. Its branch fact types are validated as arrow
            // members at build time (FLOWIP-120h), and the handler performs
            // the guarded wrapper so the branch is explicit in the type.
            // Branch facts ride the effect cursor as recorded outcomes, so
            // contracts stay strict with no breaker-aware compensation:
            // every admitted input has a journaled outcome whether the
            // gateway ran or the breaker synthesized it.
            authorize_payment = effectful_transform!(
                ValidatedOrder -> {
                    PaymentAuthorized,
                    PaymentDeclined,
                    GatewayPaymentFallback,
                    GatewayPaymentRejected,
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
            // gateway call or breaker fallback. No payment decision was
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
