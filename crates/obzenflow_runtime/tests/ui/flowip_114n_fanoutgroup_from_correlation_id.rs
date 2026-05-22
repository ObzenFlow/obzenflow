// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::CorrelationId;
use obzenflow_core::EventId;
use obzenflow_runtime::testing::ParentEventId;

fn main() {
    let correlation_id = CorrelationId::new();
    let event_id: EventId = correlation_id.into_ulid().into();

    // Correlation IDs must not be usable as a fan-out grouping key. A ParentEventId
    // can only be obtained from an observed envelope id, not constructed from an
    // arbitrary ID value.
    let _parent = ParentEventId(event_id);
}

