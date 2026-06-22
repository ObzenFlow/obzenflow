// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::LoggingMiddleware;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime::{
    HandlerObserver, HandlerObserverContext, JoinObserver, JoinObserverContext,
    ObserverDeterminism, ObserverReport, SinkDeliveryObserver, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome, SourcePollObserver, SourcePollObserverContext, StatefulObserver,
    StatefulObserverContext,
};

impl HandlerObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        self.log_processing(ctx.input);
        ObserverReport::empty()
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.log_completed(ctx.input, outputs.len());
        ObserverReport::empty()
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
            self.log_processing(input);
        }
        ObserverReport::empty()
    }

    fn after_state_emit(
        &self,
        ctx: &StatefulObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.log_completed(input, outputs.len());
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
            self.log_processing(input);
        }
        ObserverReport::empty()
    }

    fn after_join_output(
        &self,
        ctx: &JoinObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.log_completed(input, outputs.len());
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
        if !outputs.is_empty() {
            self.add_processed(outputs.iter().filter(|event| event.is_data()).count());
            self.emit(format!("Source poll produced {} events", outputs.len()));
        }
        ObserverReport::empty()
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
        self.log_processing(ctx.input);
        match &ctx.outcome {
            SinkDeliveryObserverOutcome::Delivered => {
                self.emit(format!("Sink delivered {}", ctx.input.id));
            }
            SinkDeliveryObserverOutcome::Failed { message } => {
                self.emit(format!(
                    "Sink delivery failed for {}: {}",
                    ctx.input.id, message
                ));
            }
            SinkDeliveryObserverOutcome::Rejected { reason } => {
                self.emit(format!(
                    "Sink delivery rejected for {}: {}",
                    ctx.input.id, reason
                ));
            }
        }
        ObserverReport::empty()
    }
}
