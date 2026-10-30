// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The payment-gateway resilience flow.
//!
//! Two upstream order channels, four business deliveries:
//!
//! ```text
//! web_orders ---+ canonical merge
//! store_orders -+-> validate_order -- ValidatedOrder --> authorize_payment -- PaymentAuthorized --> paid-orders sink
//!                     |                                                   |-- PaymentDeclined --> declined-orders sink
//!                     |                                                   \-- PaymentAuthorizationUnavailable --> manual-review sink
//!                     \-- CancelledOrder --> cancelled-orders sink
//! ```
//!
//! The channel merge at `validate_order` is FLOWIP-095d's canonical
//! deterministic merge, enabled automatically because the effectful
//! `authorize_payment` sits below the fan-in. Delivery order is a pure
//! function of the two recorded streams: live runs with different arrival
//! timing and replays all consume orders in the same merged order.
//!
//! Validation lives in `validation.rs` as one typed classifier with the flat
//! contract `CustomerOrderPlaced -> { ValidatedOrder, InvalidOrder,
//! CancelledOrder }`. Its handler-side `ValidationOutcome` carrier is proven
//! leaf-equal and never journalled; the `authorize_payment` stage performs the
//! `payment.authorize` effect (see
//! `gateway.rs`). On a live run the configured resilience policy may make up
//! to three breaker-governed physical calls before the runtime journals one
//! terminal result; on replay the runtime returns that recorded gateway
//! decision without calling the gateway again. That decision is the named
//! payment fact, authorized or declined, and each reaches its own delivery.
//!
//! `InvalidOrder` is a journal-recorded fact with no dedicated sink: the
//! journal is the record, sinks are external deliveries. Its consequence,
//! `CancelledOrder`, reaches the cancelled-orders sink. A decline is a payment
//! outcome, not a cancellation; a subscriber that wants one feed of failed
//! orders joins the cancelled and declined feeds downstream.
//!
//! The resilience unit on the authorize_payment stage is the second,
//! independent layer: bounded retry handles transient failures, the circuit
//! breaker fails fast once the dependency looks unhealthy, and a per-attempt
//! limiter governs every physical gateway call. Unavailability deliberately
//! does not cancel; no decision was reached, so those orders go to manual
//! review. See `README.md`.

use super::console;
use super::deliveries::{ManualReviewDeliveryLog, ShippingDeliveryLog};
use super::domain::{
    CancelledOrder, CustomerOrderPlaced, InvalidOrder, PaymentAuthorizationUnavailable,
    PaymentAuthorized, PaymentDeclined, ValidatedOrder,
};
use super::fixtures;
use super::gateway::{GatewayTransform, ScriptedGateway};
use super::validation;
use obzenflow::effects::simulated::CallLogError;
use obzenflow::effects::{EffectPortResolutionError, System};
use obzenflow::flow::{effectful_transform, flow, sink, source, transform};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::{circuit_breaker, rate_limit, retry, sink_delivery_observer};
use obzenflow::stages::sinks::ConsoleSink;
use obzenflow::stages::sources;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const SOURCE_RATE_LIMIT_EVENTS_PER_SECOND: f64 = 20.0;
const SOURCE_RATE_LIMIT_BURST: f64 = 1.0;
const GATEWAY_CALLS_PER_SECOND: f64 = 1.0;
const GATEWAY_BREAKER_OPEN_FOR: Duration = Duration::from_secs(5);
const FIRST_RECOVERY_PAUSE: Duration = Duration::from_millis(5_250);

/// Where the demo binary journals its runs.
pub(super) const DEMO_JOURNAL_ROOT: &str = "target/payment-gateway-logs";

/// Where the demo binary's scripted gateway keeps its calls.
pub(super) const DEMO_GATEWAY_CALLS: &str = "target/gateway-calls.jsonl";

/// Build the tutorial flow: the scripted three-phase order channels, the real
/// gateway transform, and its breaker-retry-limiter policy.
///
/// Gentle source and gateway-effect rate limits keep logs and metrics readable,
/// so you can watch source-boundary and effect-boundary policy metrics change
/// over time.
pub fn build_flow() -> obzenflow::flow::FlowDefinition {
    assemble_flow(
        fixtures::scripted_web_orders(),
        fixtures::scripted_store_orders(),
        Some(FIRST_RECOVERY_PAUSE),
        GATEWAY_CALLS_PER_SECOND,
        PathBuf::from(DEMO_JOURNAL_ROOT),
        PathBuf::from(DEMO_GATEWAY_CALLS),
    )
}

/// Assemble the tutorial flow from its variable ingredients.
///
/// Journal tests reuse this entry point with their own orders, gateway pacing,
/// journal root and gateway call log while retaining the tutorial's configured
/// resilience policy.
pub fn assemble_flow(
    scripted_web_orders: Vec<CustomerOrderPlaced>,
    scripted_store_orders: Vec<CustomerOrderPlaced>,
    first_recovery_pause: Option<Duration>,
    gateway_calls_per_second: f64,
    journal_root: PathBuf,
    gateway_calls: PathBuf,
) -> obzenflow::flow::FlowDefinition {
    obzenflow::flow::FlowDefinition::materialize(move |_runtime_config| {
        let calls = gateway_calls.clone();
        let gateway = System::deferred("scripted-gateway", move || {
            let gateway = ScriptedGateway::open(&calls).map_err(gateway_unavailable)?;
            Ok(Arc::new(gateway))
        })?;
        let gateway_transform = match first_recovery_pause {
            Some(pause) => GatewayTransform::new(gateway).with_first_recovery_pause(pause),
            None => GatewayTransform::new(gateway),
        };
        // One effect-only resilience attachment owns the gateway's health,
        // recovery policy, and per-physical-attempt admission. Its fixed ordering
        // keeps limiter wait outside the dependency clock and prevents a stale
        // breaker decision from surviving the queue.
        let gateway_breaker = circuit_breaker()
            .count_window(5)
            .minimum_calls(5)
            .failure_rate_threshold(0.6)
            .slow_call_duration(Duration::from_millis(250))
            .slow_call_rate_threshold(0.5)
            .open_for(GATEWAY_BREAKER_OPEN_FOR)
            .probes(1);
        let gateway_limiter = rate_limit(gateway_calls_per_second);
        let gateway_retry = retry()
            .fixed_delay(Duration::from_millis(250))
            .max_attempts(3)
            .attempt_start_window(Duration::from_secs(30));

        let web_orders_feed = sources::ValuesSource::new(scripted_web_orders);
        let store_orders_feed = sources::ValuesSource::new(scripted_store_orders);
        let validate_order = validation::ValidateOrder;
        let shipping_handoff = ConsoleSink::new(console::format_shipping);
        let record_declined = ConsoleSink::new(console::format_declined);
        let record_cancelled = ConsoleSink::new(console::format_cancelled);
        let record_unavailable = ConsoleSink::new(console::format_unavailable);

        Ok(flow! {
            name: "payment_gateway_resilience_demo",
            journals: disk_journals(journal_root),

            stages: {
                // Sources: two scripted order channels across three phases
                // (warmup, outage, recovery), of deliberately unequal length.
                //
                // The flow reacts to these facts. On replay the runtime injects
                // journaled source events instead of polling these sources again.
                //
                // The source rate limiter is the source-boundary example
                // (FLOWIP-115a). These are local scripted fixtures, so a source
                // circuit breaker would be misleading here; the breaker belongs
                // on external dependencies such as the gateway effect below.
                web_orders = source!(CustomerOrderPlaced with {
                    rate_limit(SOURCE_RATE_LIMIT_EVENTS_PER_SECOND)
                        .burst_capacity(SOURCE_RATE_LIMIT_BURST)
                } => web_orders_feed);
                store_orders = source!(CustomerOrderPlaced with {
                    rate_limit(SOURCE_RATE_LIMIT_EVENTS_PER_SECOND)
                        .burst_capacity(SOURCE_RATE_LIMIT_BURST)
                } => store_orders_feed);

                // Local validation: deterministic checks with no external I/O,
                // classified exactly once by one multi-type stage. This is typed
                // business classification, not exception handling: a valid order
                // becomes `ValidatedOrder`, an invalid order records the
                // `InvalidOrder` fact and its derived `CancelledOrder` consequence.
                // Its typed carrier lowers directly to the declared flat facts;
                // no effect cursor or effect provenance is involved.
                validate_order = transform!(
                    CustomerOrderPlaced -> {
                        ValidatedOrder,
                        InvalidOrder,
                        CancelledOrder
                    } => validate_order
                );

                // Payment authorization: the only stage that touches the outside
                // world, expressed as the `payment.authorize` effect. The runtime
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
                // The finite demo pauses before its first live Recovery call so
                // the real five-second open window expires before that probe.
                // The handler performs one effect, so the controls in `with`
                // guard that effect: one policy instance per protected
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
                        PaymentAuthorizationUnavailable,
                    }
                    with { gateway_breaker, gateway_retry, gateway_limiter }
                    => gateway_transform,
                );

                // Paid-order sink: a tiny in-process console integration. Its
                // implementation only writes and reports what it actually did;
                // `.idempotent()` classifies repeat delivery for resume.
                // One passive observer logs the runtime's immutable delivery
                // classification. It cannot alter settlement or emit flow facts.
                paid_orders = sink!(
                    PaymentAuthorized with { sink_delivery_observer(
                        "shipping-delivery-log",
                        ShippingDeliveryLog
                    ) } => shipping_handoff.idempotent()
                );

                // Declined-payment console destination. The gateway's decline
                // is recorded by the effect as `PaymentDeclined`; it is a
                // payment outcome, not an order cancellation.
                declined_orders = sink!(PaymentDeclined => record_declined.idempotent());

                // Cancelled-order console destination for locally invalid
                // orders. `InvalidOrder` stays a journal-recorded fact with no
                // dedicated sink; this delivery carries its lifecycle
                // consequence. The console adapter labels reconstructed output.
                cancelled_orders = sink!(CancelledOrder => record_cancelled.idempotent());

                // Unavailable-authorization destination: failed gateway call or
                // breaker refusal. No payment decision was reached, so the order
                // is not cancelled; it goes to manual review. A passive observer
                // logs routing after successful output; the Delivery receipt
                // remains the durable settlement fact.
                manual_review = sink!(
                    PaymentAuthorizationUnavailable with { sink_delivery_observer(
                        "manual-review-delivery-log",
                        ManualReviewDeliveryLog
                    ) } => record_unavailable.idempotent()
                );
            },

            topology: {
                web_orders |> validate_order;
                store_orders |> validate_order;
                validate_order |> authorize_payment;
                validate_order |> cancelled_orders;
                authorize_payment |> paid_orders;
                authorize_payment |> declined_orders;
                authorize_payment |> manual_review;
            }
        })
    })
}

fn gateway_unavailable(error: CallLogError) -> EffectPortResolutionError {
    tracing::error!(%error, "the scripted gateway could not open its call log");
    EffectPortResolutionError::ClientConstructionFailed
}
