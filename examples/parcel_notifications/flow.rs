// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{self, EmailSent, LocatedParcel, ParcelUpdate, SmsSent};
use super::handlers::{LocateParcel, NotifyCustomer};
use super::services::{EmailNotice, SimulatedDepots, SimulatedMessaging, SmsNotice};
use obzenflow::effects::Port;
use obzenflow::flow::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::{circuit_breaker, rate_limit, retry, MiddlewareFactory};
use obzenflow::stages::sinks::ConsoleSink;
use obzenflow::stages::sources;
use std::path::PathBuf;
use std::time::Duration;

const JOURNAL_ROOT: &str = "target/parcel-notifications-logs";

pub fn build_flow() -> FlowDefinition {
    FlowDefinition::materialize(|_runtime_config| {
        let carrier_limiter = rate_limit(5.0).burst_capacity(2.0);

        let lookup_limiter = rate_limit(10.0);
        let lookup_breaker = circuit_breaker()
            .count_window(20)
            .minimum_calls(10)
            .failure_rate_threshold(0.5)
            .slow_call_duration(Duration::from_millis(250))
            .slow_call_rate_threshold(0.8);
        let lookup_retry = retry()
            .fixed_delay(Duration::from_millis(200))
            .max_attempts(3);

        let email_limiter = rate_limit(4.0).for_effect::<EmailNotice>();
        let email_breaker = circuit_breaker()
            .count_window(10)
            .minimum_calls(5)
            .failure_rate_threshold(0.5)
            .for_effect::<EmailNotice>();
        let email_retry = retry()
            .fixed_delay(Duration::from_millis(500))
            .max_attempts(4)
            .for_effect::<EmailNotice>();
        let sms_limiter = rate_limit(1.0).for_effect::<SmsNotice>();

        let updates = sources::ValuesSource::new(domain::carrier_updates());
        let locate = LocateParcel::new(Port::to(SimulatedDepots));
        let notify = NotifyCustomer::new(Port::to(SimulatedMessaging));
        let emails = ConsoleSink::new(domain::format_email);
        let texts = ConsoleSink::new(domain::format_sms);

        Ok(flow! {
            name: "parcel_notifications",
            journals: disk_journals(PathBuf::from(JOURNAL_ROOT)),

            stages: {
                carrier_updates = source!(ParcelUpdate with { carrier_limiter } => updates);
                locate_parcel = effectful_transform!(
                    ParcelUpdate -> LocatedParcel
                    with { lookup_limiter, lookup_breaker, lookup_retry }
                    => locate
                );
                notify_customer = effectful_transform!(
                    LocatedParcel -> { EmailSent, SmsSent }
                    with { email_limiter, email_breaker, email_retry, sms_limiter }
                    => notify
                );
                sent_emails = sink!(EmailSent => emails.idempotent());
                sent_texts = sink!(SmsSent => texts.idempotent());
            },

            topology: {
                carrier_updates |> locate_parcel;
                locate_parcel |> notify_customer;
                notify_customer |> sent_emails;
                notify_customer |> sent_texts;
            }
        })
    })
}
