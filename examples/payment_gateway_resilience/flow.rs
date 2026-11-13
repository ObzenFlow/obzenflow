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
use super::gateway::{simulated_processor, AuthorizePayment, PaymentGatewayPort};
use super::validation;
use obzenflow::effects::Port;
use obzenflow::flow::{effectful_transform, flow, sink, source, transform, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::{circuit_breaker, rate_limit, retry, sink_delivery_observer};
use obzenflow::stages::sinks::ConsoleSink;
use obzenflow::stages::sources;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

const SOURCE_RATE_LIMIT_EVENTS_PER_SECOND: f64 = 20.0;
const SOURCE_RATE_LIMIT_BURST: f64 = 1.0;
const AUTHORIZE_CALLS_PER_SECOND: f64 = 1.0;
const AUTHORIZE_BREAKER_OPEN_FOR: Duration = Duration::from_secs(5);
const AUTHORIZE_BREAKER_OPEN_FOR_MULTIPLIER: f64 = 2.0;
const AUTHORIZE_BREAKER_MAX_OPEN_FOR: Duration = Duration::from_secs(20);
const FIRST_RECOVERY_PAUSE: Duration = Duration::from_millis(5_250);

pub(super) const DEMO_JOURNAL_ROOT: &str = "target/payment-gateway-logs";

/// The system behind the payment gateway port, chosen by `processor` in the config's `[app]`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Processor {
    Simulated,
}

/// The `[app]` table of this example's obzenflow.toml.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PaymentSettings {
    processor: Processor,
    processor_calls: PathBuf,
}

pub fn build_flow() -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, settings: PaymentSettings| {
        let port = match settings.processor {
            Processor::Simulated => simulated_processor(&settings.processor_calls),
        };
        Ok(assemble_flow(
            fixtures::scripted_web_orders(),
            fixtures::scripted_store_orders(),
            Some(FIRST_RECOVERY_PAUSE),
            AUTHORIZE_CALLS_PER_SECOND,
            PathBuf::from(DEMO_JOURNAL_ROOT),
            port,
        ))
    })
}

pub fn assemble_flow(
    scripted_web_orders: Vec<CustomerOrderPlaced>,
    scripted_store_orders: Vec<CustomerOrderPlaced>,
    first_recovery_pause: Option<Duration>,
    authorize_calls_per_second: f64,
    journal_root: PathBuf,
    port: Port<dyn PaymentGatewayPort>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let authorize = AuthorizePayment::new(port);
        let authorize = match first_recovery_pause {
            Some(pause) => authorize.with_first_recovery_pause(pause),
            None => authorize,
        };
        let authorize_breaker = circuit_breaker()
            .count_window(5)
            .minimum_calls(5)
            .failure_rate_threshold(0.6)
            .slow_call_duration(Duration::from_millis(250))
            .slow_call_rate_threshold(0.5)
            .open_for(AUTHORIZE_BREAKER_OPEN_FOR)
            .open_for_multiplier(AUTHORIZE_BREAKER_OPEN_FOR_MULTIPLIER)
            .max_open_for(AUTHORIZE_BREAKER_MAX_OPEN_FOR)
            .probes(1);
        let authorize_limiter = rate_limit(authorize_calls_per_second);
        let authorize_retry = retry()
            .fixed_delay(Duration::from_millis(250))
            .max_attempts(3)
            .attempt_start_window(Duration::from_secs(30));

        let web_orders_feed = sources::ValuesSource::new(scripted_web_orders);
        let store_orders_feed = sources::ValuesSource::new(scripted_store_orders);
        let validate_order = validation::ValidateOrder;
        let shipping_handoff = ConsoleSink::new(console::format_shipping);
        let record_declined = ConsoleSink::new(console::format_declined);
        let record_invalid = ConsoleSink::new(console::format_invalid);
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
                    with {
                        authorize_breaker,
                        authorize_retry,
                        authorize_limiter,
                    }
                    => authorize,
                );

                paid_orders = sink!(
                    PaymentAuthorized with { sink_delivery_observer(
                        "shipping-delivery-log",
                        ShippingDeliveryLog
                    ) } => shipping_handoff.idempotent()
                );
                declined_orders = sink!(PaymentDeclined => record_declined.idempotent());
                invalid_orders = sink!(InvalidOrder => record_invalid.idempotent());
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
                validate_order |> invalid_orders;
                validate_order |> cancelled_orders;
                authorize_payment |> paid_orders;
                authorize_payment |> declined_orders;
                authorize_payment |> manual_review;
            }
        })
    })
}
