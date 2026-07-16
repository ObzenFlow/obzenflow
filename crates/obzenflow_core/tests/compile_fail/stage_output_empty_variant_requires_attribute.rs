// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// A zero-fact filter arm must be deliberate: a bare unit variant is
// rejected so filtering cannot be accidental; spell it
// `#[stage_output(empty)]` (FLOWIP-120z).

use obzenflow_core::{StageOutputFacts, TypedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirstFact;

impl TypedPayload for FirstFact {
    const EVENT_TYPE: &'static str = "compile_fail.first";
}

#[derive(Debug, Clone, StageOutputFacts)]
enum AccidentalFilter {
    Present(FirstFact),
    Skipped,
}

fn main() {}
