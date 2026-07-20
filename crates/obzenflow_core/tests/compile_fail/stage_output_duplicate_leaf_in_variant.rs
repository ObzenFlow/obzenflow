// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// A repeated leaf type within one variant would serialize two facts of one
// event type; no recorded group could reconstruct them unambiguously
// (FLOWIP-120z).

use obzenflow_core::{StageOutputFacts, TypedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirstFact;

impl TypedPayload for FirstFact {
    const EVENT_TYPE: &'static str = "compile_fail.first";
}

#[derive(Debug, Clone, StageOutputFacts)]
enum RepeatedLeaf {
    Doubled {
        original: FirstFact,
        duplicate: FirstFact,
    },
}

fn main() {}
