// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::LoggingMiddleware;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::{
    LoggingInputReference, LoggingJoinCanonicalMerge, LoggingJoinDelivery, LoggingJoinSide,
    LoggingOccurrence, LoggingSinkAttemptResult, LoggingSinkOutcome, LoggingSourceOutcome,
};
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, JoinObserver, JoinObserverContext, JoinSide,
    ObserverDeterminism, ObserverReport, SinkDeliveryAttemptResult, SinkDeliveryObserver,
    SinkDeliveryObserverContext, SinkDeliveryObserverOutcome, SourcePollObserver,
    SourcePollObserverContext, SourcePollObserverOutcome, StatefulObserver,
    StatefulObserverContext,
};

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn input_reference(event: &ChainEvent, stage_input_position: Option<u64>) -> LoggingInputReference {
    LoggingInputReference {
        event_id: event.id,
        event_type: event.event_type(),
        stage_input_position,
    }
}

fn join_delivery(ctx: &JoinObserverContext<'_>) -> Option<LoggingJoinDelivery> {
    let delivery = ctx.delivery?;
    Some(LoggingJoinDelivery {
        side: match delivery.side {
            JoinSide::Reference => LoggingJoinSide::Reference,
            JoinSide::Stream => LoggingJoinSide::Stream,
        },
        source_stage_id: delivery.delivered_source_stage_id,
        stage_input_position: delivery.delivered_stage_input_position,
        reference_high_water: delivery.reference_high_water.clone(),
        canonical_merge: delivery.canonical_merge.as_ref().map(|metadata| {
            LoggingJoinCanonicalMerge {
                selected_feed: metadata.selected_feed.clone(),
                reader_index: metadata.reader_index,
            }
        }),
    })
}

impl HandlerObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty().with_diagnostic(self.diagnostic(
            LoggingOccurrence::HandlerInputObserved {
                input: input_reference(ctx.input, ctx.stage_input_position),
            },
        ))
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        outputs: &[ChainEvent],
    ) -> ObserverReport {
        ObserverReport::empty().with_diagnostic(self.diagnostic(
            LoggingOccurrence::HandlerOutputObserved {
                input: input_reference(ctx.input, ctx.stage_input_position),
                output_count: count(outputs.len()),
            },
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
        let Some(input) = ctx.input else {
            return ObserverReport::empty();
        };
        ObserverReport::empty().with_diagnostic(self.diagnostic(
            LoggingOccurrence::StatefulInputObserved {
                input: input_reference(input, ctx.stage_input_position),
            },
        ))
    }

    fn after_state_emit(
        &self,
        ctx: &StatefulObserverContext<'_>,
        outputs: &[ChainEvent],
    ) -> ObserverReport {
        let Some(input) = ctx.input else {
            return ObserverReport::empty();
        };
        ObserverReport::empty().with_diagnostic(self.diagnostic(
            LoggingOccurrence::StatefulOutputObserved {
                input: input_reference(input, ctx.stage_input_position),
                output_count: count(outputs.len()),
            },
        ))
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
        let (Some(input), Some(delivery)) = (ctx.input, join_delivery(ctx)) else {
            return ObserverReport::empty();
        };
        ObserverReport::empty().with_diagnostic(self.diagnostic(
            LoggingOccurrence::JoinInputObserved {
                input: input_reference(input, Some(delivery.stage_input_position)),
                delivery,
            },
        ))
    }

    fn after_join_output(
        &self,
        ctx: &JoinObserverContext<'_>,
        outputs: &[ChainEvent],
    ) -> ObserverReport {
        let (Some(input), Some(delivery)) = (ctx.input, join_delivery(ctx)) else {
            return ObserverReport::empty();
        };
        ObserverReport::empty().with_diagnostic(self.diagnostic(
            LoggingOccurrence::JoinOutputObserved {
                input: input_reference(input, Some(delivery.stage_input_position)),
                delivery,
                output_count: count(outputs.len()),
            },
        ))
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
        ctx: &SourcePollObserverContext<'_>,
        outputs: &[ChainEvent],
    ) -> ObserverReport {
        let outcome = match &ctx.outcome {
            SourcePollObserverOutcome::Batch { events } => LoggingSourceOutcome::Batch {
                events: count(*events),
            },
            SourcePollObserverOutcome::Eof => LoggingSourceOutcome::Eof,
            SourcePollObserverOutcome::Error { kind } => {
                LoggingSourceOutcome::Error { kind: kind.clone() }
            }
            SourcePollObserverOutcome::Rejected { policy } => LoggingSourceOutcome::Rejected {
                policy: policy.clone(),
            },
        };
        ObserverReport::empty().with_diagnostic(self.diagnostic(
            LoggingOccurrence::SourcePollObserved {
                poll_duration_ms: millis(ctx.poll_duration),
                output_count: count(outputs.len()),
                data_event_count: count(outputs.iter().filter(|event| event.is_data()).count()),
                outcome,
            },
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
        let outcome = match &ctx.outcome {
            SinkDeliveryObserverOutcome::Attempted { result } => LoggingSinkOutcome::Attempted {
                result: match result {
                    SinkDeliveryAttemptResult::ReportedSuccess => {
                        LoggingSinkAttemptResult::ReportedSuccess
                    }
                    SinkDeliveryAttemptResult::ReportedPartial {
                        successful_count,
                        failed_count,
                    } => LoggingSinkAttemptResult::ReportedPartial {
                        successful_count: *successful_count,
                        failed_count: *failed_count,
                    },
                    SinkDeliveryAttemptResult::ReportedBuffered => {
                        LoggingSinkAttemptResult::ReportedBuffered
                    }
                    SinkDeliveryAttemptResult::ReportedFailure { final_attempt } => {
                        LoggingSinkAttemptResult::ReportedFailure {
                            final_attempt: *final_attempt,
                        }
                    }
                    SinkDeliveryAttemptResult::HandlerError { kind } => {
                        LoggingSinkAttemptResult::HandlerError { kind: kind.clone() }
                    }
                    SinkDeliveryAttemptResult::HandlerPanicked => {
                        LoggingSinkAttemptResult::HandlerPanicked
                    }
                },
            },
            SinkDeliveryObserverOutcome::Rejected { policy } => LoggingSinkOutcome::Rejected {
                policy: policy.clone(),
            },
        };
        ObserverReport::empty().with_diagnostic(self.diagnostic(
            LoggingOccurrence::SinkDeliveryBoundaryObserved {
                input: input_reference(ctx.input, ctx.stage_input_position),
                outcome,
            },
        ))
    }
}
