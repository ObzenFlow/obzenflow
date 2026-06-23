// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Observer hook implementation for the indicator middleware (FLOWIP-115f).
//!
//! The latency indicator attaches to the handler surface. `after_handle` runs
//! once per input (the runtime dispatches it once per handler invocation with
//! the full output slice), so emitting one sample there is inherently one
//! durable sample per operation execution, independent of handler fan-out. The
//! observer never mutates outputs: the value-preserving `processing_time` stamp
//! is applied by the runtime output committer, not this observer.

use super::IndicatorMiddleware;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, ObserverDeterminism, ObserverReport,
};

impl HandlerObserver for IndicatorMiddleware {
    fn label(&self) -> &'static str {
        "indicator"
    }

    fn determinism(&self) -> ObserverDeterminism {
        // Wall-clock measurement is live-only; strict replay suppresses the
        // measurement, and the sample recorded during the live run is not
        // re-emitted (it remains in the original journal).
        ObserverDeterminism::LiveOnly
    }

    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        self.remember_start(ctx.input);
        ObserverReport::empty()
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        _outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        // One sample per execution, recording the raw measured duration. The
        // objective/threshold is applied read-side (FLOWIP-115l), not here.
        let value = self.duration_for_input(ctx.input);
        ObserverReport::empty().with_diagnostic(self.diagnostic(ctx.stage_id, value))
    }
}
