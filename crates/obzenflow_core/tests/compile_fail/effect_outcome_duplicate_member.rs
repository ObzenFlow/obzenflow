// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// Dispatch is by event type, so the same member type twice would make
// reconstruction ambiguous; the derive rejects the literal repeat
// (FLOWIP-120m). Distinct types colliding on EVENT_TYPE are caught at flow
// build instead.

use obzenflow_core::{EffectOutcomeFacts, TypedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirstFact;

impl TypedPayload for FirstFact {
    const EVENT_TYPE: &'static str = "compile_fail.first";
}

#[derive(Debug, Clone, EffectOutcomeFacts)]
pub enum RepeatedMember {
    Original(FirstFact),
    Retry(FirstFact),
}

fn main() {}
