// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Current reporting values carried by existing journal records. These keys
//! select replacements, never a sequence of facts to count or replay.

use crate::metrics::ContractMetricEdgeKey;
use crate::{StageId, WriterId};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MetricsTailKey {
    Accounting(StageId),
    StageLifecycle(StageId),
    PipelineLifecycle(WriterId),
    PipelineOutcome(WriterId),
    CircuitBreaker(StageId),
    HttpPull(StageId),
    Contract(ContractMetricEdgeKey),
}
