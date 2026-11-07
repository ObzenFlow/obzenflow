// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

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
use obzenflow::effects::{EffectPortResolutionError, Port};
use obzenflow::flow::{effectful_transform, flow, sink, source, transform, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::{circuit_breaker, rate_limit, retry, sink_delivery_observer};
use obzenflow::stages::sinks::ConsoleSink;
use obzenflow::stages::sources;
use std::path::PathBuf;
use std::time::Duration;

const SOURCE_RATE_LIMIT_EVENTS_PER_SECOND: f64 = 20.0;
const SOURCE_RATE_LIMIT_BURST: f64 = 1.0;
const GATEWAY_CALLS_PER_SECOND: f64 = 1.0;
const GATEWAY_BREAKER_OPEN_FOR: Duration = Duration::from_secs(5);
const FIRST_RECOVERY_PAUSE: Duration = Duration::from_millis(5_250);

pub(super) const DEMO_JOURNAL_ROOT: &str = "target/payment-gateway-logs";

pub(super) const DEMO_GATEWAY_CALLS: &str = "target/gateway-calls.jsonl";

pub fn build_flow() -> FlowDefinition {
    assemble_flow(
        fixtures::scripted_web_orders(),
        fixtures::scripted_store_orders(),
        Some(FIRST_RECOVERY_PAUSE),
        GATEWAY_CALLS_PER_SECOND,
        PathBuf::from(DEMO_JOURNAL_ROOT),
        PathBuf::from(DEMO_GATEWAY_CALLS),
    )
}

pub fn assemble_flow(
    scripted_web_orders: Vec<CustomerOrderPlaced>,
    scripted_store_orders: Vec<CustomerOrderPlaced>,
    first_recovery_pause: Option<Duration>,
    gateway_calls_per_second: f64,
    journal_root: PathBuf,
    gateway_calls: PathBuf,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let calls = gateway_calls.clone();
        let gateway =
            Port::deferred(move || ScriptedGateway::open(&calls).map_err(gateway_unavailable));
        let gateway_transform = match first_recovery_pause {
            Some(pause) => GatewayTransform::new(gateway).with_first_recovery_pause(pause),
            None => GatewayTransform::new(gateway),
        };
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
                web_orders = source!(CustomerOrderPlaced with {
                    rate_limit(SOURCE_RATE_LIMIT_EVENTS_PER_SECOND)
                        .burst_capacity(SOURCE_RATE_LIMIT_BURST)
                } => web_orders_feed);
                store_orders = source!(CustomerOrderPlaced with {
                    rate_limit(SOURCE_RATE_LIMIT_EVENTS_PER_SECOND)
                        .burst_capacity(SOURCE_RATE_LIMIT_BURST)
                } => store_orders_feed);

                validate_order = transform!(
                    CustomerOrderPlaced -> {
                        ValidatedOrder,
                        InvalidOrder,
                        CancelledOrder
                    } => validate_order
                );

                authorize_payment = effectful_transform!(
                    ValidatedOrder -> {
                        PaymentAuthorized,
                        PaymentDeclined,
                        PaymentAuthorizationUnavailable,
                    }
                    with { gateway_breaker, gateway_retry, gateway_limiter }
                    => gateway_transform,
                );

                paid_orders = sink!(
                    PaymentAuthorized with { sink_delivery_observer(
                        "shipping-delivery-log",
                        ShippingDeliveryLog
                    ) } => shipping_handoff.idempotent()
                );
                declined_orders = sink!(PaymentDeclined => record_declined.idempotent());
                cancelled_orders = sink!(CancelledOrder => record_cancelled.idempotent());
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
