// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::unsynced_candidate::admit_unsynced;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{CandidateSet, ResolvedRuntimeConfig};
use std::sync::Arc;

pub fn unsynced_context() -> FlowBuildContext {
    let mut candidates = CandidateSet::default();
    admit_unsynced(&mut candidates);
    FlowBuildContext::new(Arc::new(ResolvedRuntimeConfig::new(candidates)))
}
