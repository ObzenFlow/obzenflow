// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// Reconstruction dispatch is by leaf set, so two variants with an identical
// leaf-type set (order-insensitively) can never be told apart from a
// recorded group (FLOWIP-120z).

use obzenflow_core::{StageOutputFacts, TypedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirstFact;

impl TypedPayload for FirstFact {
    const EVENT_TYPE: &'static str = "compile_fail.first";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecondFact;

impl TypedPayload for SecondFact {
    const EVENT_TYPE: &'static str = "compile_fail.second";
}

#[derive(Debug, Clone, StageOutputFacts)]
enum AmbiguousOutcome {
    Forward {
        first: FirstFact,
        second: SecondFact,
    },
    Backward {
        second: SecondFact,
        first: FirstFact,
    },
}

fn main() {}
