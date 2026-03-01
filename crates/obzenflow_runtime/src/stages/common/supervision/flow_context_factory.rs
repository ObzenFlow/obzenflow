// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Factory for constructing `FlowContext` from common supervisor fields.
//!
//! Every supervisor constructs `FlowContext` identically from the same five
//! fields. This helper eliminates ~20 identical 6-line constructions in the
//! transform supervisor alone.

use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::StageId;

/// Construct a `FlowContext` from the fields every supervisor has.
pub(crate) fn make_flow_context(
    flow_name: &str,
    flow_id: &str,
    stage_name: &str,
    stage_id: StageId,
    stage_type: StageType,
) -> FlowContext {
    FlowContext {
        flow_name: flow_name.to_owned(),
        flow_id: flow_id.to_owned(),
        stage_name: stage_name.to_owned(),
        stage_id,
        stage_type,
    }
}
