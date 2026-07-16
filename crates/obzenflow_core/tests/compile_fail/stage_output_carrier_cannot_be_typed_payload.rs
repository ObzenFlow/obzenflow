// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// A carrier must never double as a persisted wrapper event: implementing
// TypedPayload on a stage output carrier collides with the scalar blanket
// implementations, deliberately (FLOWIP-120z, inheriting FLOWIP-120m's
// mutual exclusion).

use obzenflow_core::{StageOutputFacts, TypedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirstFact;

impl TypedPayload for FirstFact {
    const EVENT_TYPE: &'static str = "compile_fail.first";
}

#[derive(Debug, Clone, Serialize, Deserialize, StageOutputFacts)]
pub enum WrapperAttempt {
    Present(FirstFact),
}

impl TypedPayload for WrapperAttempt {
    const EVENT_TYPE: &'static str = "compile_fail.wrapper";
}

fn main() {}
