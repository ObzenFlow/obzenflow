// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// Duplicate members in an explicit stage fact set are compile errors: the
// membership index would be ambiguous and the declaration lies about the
// set's cardinality (FLOWIP-120z).

use obzenflow_core::event::schema::assert_distinct_stage_fact_set;
use obzenflow_core::{stage_fact_set, TypedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirstFact;

impl TypedPayload for FirstFact {
    const EVENT_TYPE: &'static str = "compile_fail.first";
}

const _: () = assert_distinct_stage_fact_set::<stage_fact_set![FirstFact, FirstFact]>();

fn main() {}
