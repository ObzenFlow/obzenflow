// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::{effect_outcome, TypedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirstFact;

impl TypedPayload for FirstFact {
    const EVENT_TYPE: &'static str = "compile_fail.first";
}

effect_outcome! {
    #[derive(Debug, Clone)]
    pub enum UnitVariant {
        Present(FirstFact),
        Nothing,
    }
}

fn main() {}
