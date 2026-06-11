// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// A carrier must never double as a persisted wrapper event: implementing
// TypedPayload on a carrier collides with the blanket
// `TypedPayload -> TypedFactSet` implementation (FLOWIP-120m).

use obzenflow_core::{effect_outcome, TypedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirstFact;

impl TypedPayload for FirstFact {
    const EVENT_TYPE: &'static str = "compile_fail.first";
}

effect_outcome! {
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum WrapperAttempt {
        Present(FirstFact),
    }
}

impl TypedPayload for WrapperAttempt {
    const EVENT_TYPE: &'static str = "compile_fail.wrapper";
}

fn main() {}
