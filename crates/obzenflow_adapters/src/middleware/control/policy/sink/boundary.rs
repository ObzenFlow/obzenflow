// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{recovery, run_once, SinkPolicy};
use crate::middleware::BoundaryRetryOwner;
use async_trait::async_trait;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryBoundary, SinkDeliveryBoundaryReport, SinkDeliveryExecutor,
};
use std::sync::Arc;

/// Sink-delivery boundary backed by a declared-order policy chain.
pub struct PerSinkDeliveryPolicyBoundary {
    policies: Arc<Vec<Arc<dyn SinkPolicy>>>,
}

impl PerSinkDeliveryPolicyBoundary {
    pub fn new(policies: Vec<Arc<dyn SinkPolicy>>) -> Self {
        Self {
            policies: Arc::new(policies),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    pub(super) fn policies(&self) -> &[Arc<dyn SinkPolicy>] {
        &self.policies
    }

    pub(super) fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        let mut owners = self
            .policies
            .iter()
            .filter_map(|policy| policy.retry_owner());
        let owner = owners.next()?;
        debug_assert!(
            owners.next().is_none(),
            "binder must reject multiple retry owners"
        );
        Some(owner)
    }
}

#[async_trait]
impl SinkDeliveryBoundary for PerSinkDeliveryPolicyBoundary {
    async fn around_sink_delivery(
        &self,
        execute: &mut dyn SinkDeliveryExecutor,
    ) -> SinkDeliveryBoundaryReport {
        run_once(self.policies(), execute).await
    }

    async fn around_retryable_sink_delivery(
        &self,
        execute: &mut dyn SinkDeliveryExecutor,
        stop: BoundaryStopReceiver,
    ) -> SinkDeliveryBoundaryReport {
        recovery::run(self, execute, stop).await
    }
}
