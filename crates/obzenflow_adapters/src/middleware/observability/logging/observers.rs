// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::LoggingMiddleware;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, JoinObserver, JoinObserverContext,
    ObserverDeterminism, ObserverReport, SinkDeliveryObserver, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome, SourcePollObserver, SourcePollObserverContext,
    SourcePollObserverOutcome, StatefulObserver, StatefulObserverContext,
};
use serde_json::json;

impl HandlerObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        let message = self.log_processing(ctx.input);
        ObserverReport::empty().with_diagnostic(self.diagnostic_event(
            ctx.stage_id,
            "before_handle",
            message,
            Some(ctx.input),
            json!({}),
        ))
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        let message = self.log_completed(ctx.input, outputs.len());
        ObserverReport::empty().with_diagnostic(self.diagnostic_event(
            ctx.stage_id,
            "after_handle",
            message,
            Some(ctx.input),
            json!({ "output_count": outputs.len() }),
        ))
    }
}

impl StatefulObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) -> ObserverReport {
        if let Some(input) = ctx.input {
            let message = self.log_processing(input);
            return ObserverReport::empty().with_diagnostic(self.diagnostic_event(
                ctx.stage_id,
                "before_state_accumulate",
                message,
                Some(input),
                json!({}),
            ));
        }
        ObserverReport::empty()
    }

    fn after_state_emit(
        &self,
        ctx: &StatefulObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        if let Some(input) = ctx.input {
            let message = self.log_completed(input, outputs.len());
            return ObserverReport::empty().with_diagnostic(self.diagnostic_event(
                ctx.stage_id,
                "after_state_emit",
                message,
                Some(input),
                json!({ "output_count": outputs.len() }),
            ));
        }
        ObserverReport::empty()
    }
}

impl JoinObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_join_input(&self, ctx: &JoinObserverContext<'_>) -> ObserverReport {
        if let Some(input) = ctx.input {
            let message = self.log_processing(input);
            return ObserverReport::empty().with_diagnostic(self.diagnostic_event(
                ctx.stage_id,
                "before_join_input",
                message,
                Some(input),
                json!({}),
            ));
        }
        ObserverReport::empty()
    }

    fn after_join_output(
        &self,
        ctx: &JoinObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        if let Some(input) = ctx.input {
            let message = self.log_completed(input, outputs.len());
            return ObserverReport::empty().with_diagnostic(self.diagnostic_event(
                ctx.stage_id,
                "after_join_output",
                message,
                Some(input),
                json!({ "output_count": outputs.len() }),
            ));
        }
        ObserverReport::empty()
    }
}

impl SourcePollObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn after_source_poll(
        &self,
        _ctx: &SourcePollObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        let data_events = outputs.iter().filter(|event| event.is_data()).count();
        self.add_processed(data_events);
        let outcome = match &_ctx.outcome {
            SourcePollObserverOutcome::Batch { events } => {
                json!({ "kind": "batch", "events": events })
            }
            SourcePollObserverOutcome::Eof => json!({ "kind": "eof" }),
            SourcePollObserverOutcome::Error { message } => {
                json!({ "kind": "error", "message": message })
            }
            SourcePollObserverOutcome::Rejected { reason } => {
                json!({ "kind": "rejected", "reason": reason })
            }
        };
        let message = format!("Source poll observed {} outputs", outputs.len());
        self.emit(message.clone());
        ObserverReport::empty().with_diagnostic(self.diagnostic_event(
            _ctx.stage_id,
            "after_source_poll",
            message,
            None,
            json!({
                "outcome": outcome,
                "output_count": outputs.len(),
                "data_event_count": data_events,
                "poll_duration_ms": _ctx.poll_duration.as_millis(),
            }),
        ))
    }
}

impl SinkDeliveryObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) -> ObserverReport {
        let processing_message = self.log_processing(ctx.input);
        let mut report = ObserverReport::empty().with_diagnostic(self.diagnostic_event(
            ctx.stage_id,
            "before_sink_delivery",
            processing_message,
            Some(ctx.input),
            json!({}),
        ));
        let (message, outcome) = match &ctx.outcome {
            SinkDeliveryObserverOutcome::Delivered => (
                format!("Sink delivered {}", ctx.input.id),
                json!({ "kind": "delivered" }),
            ),
            SinkDeliveryObserverOutcome::Failed { message } => (
                format!("Sink delivery failed for {}: {}", ctx.input.id, message),
                json!({ "kind": "failed", "message": message }),
            ),
            SinkDeliveryObserverOutcome::Rejected { reason } => (
                format!("Sink delivery rejected for {}: {}", ctx.input.id, reason),
                json!({ "kind": "rejected", "reason": reason }),
            ),
        };
        self.emit(message.clone());
        report = report.with_diagnostic(self.diagnostic_event(
            ctx.stage_id,
            "after_sink_delivery",
            message,
            Some(ctx.input),
            json!({ "outcome": outcome }),
        ));
        report
    }
}
