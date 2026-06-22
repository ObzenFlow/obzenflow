// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::TimingMiddleware;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, JoinObserver, JoinObserverContext,
    ObserverCommitResult, ObserverDeterminism, ObserverReport, OutputCommitObserver,
    OutputCommitObserverContext, SourcePollObserver, SourcePollObserverContext, StatefulObserver,
    StatefulObserverContext,
};

impl HandlerObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        self.remember_start(ctx.input);
        ObserverReport::empty()
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.stamp_outputs(Some(ctx.input), outputs);
        ObserverReport::empty()
    }
}

impl StatefulObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.remember_start(input);
        }
        ObserverReport::empty()
    }

    fn after_state_emit(
        &self,
        ctx: &StatefulObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.stamp_outputs(ctx.input, outputs);
        ObserverReport::empty()
    }
}

impl JoinObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_join_input(&self, ctx: &JoinObserverContext<'_>) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.remember_start(input);
        }
        ObserverReport::empty()
    }

    fn after_join_output(
        &self,
        ctx: &JoinObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.stamp_outputs(ctx.input, outputs);
        ObserverReport::empty()
    }
}

impl SourcePollObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn after_source_poll(
        &self,
        ctx: &SourcePollObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.stamp_source_outputs(ctx.poll_duration, outputs);
        ObserverReport::empty()
    }
}

impl OutputCommitObserver for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_output_commit(
        &self,
        _ctx: &OutputCommitObserverContext<'_>,
        event: &mut ChainEvent,
    ) -> ObserverCommitResult {
        if let Some(duration) = self.take_output_duration(event) {
            event.processing_info.processing_time = duration;
        }
        Ok(ObserverReport::empty())
    }
}
