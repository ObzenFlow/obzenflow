// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::dsl::stage_descriptor::EffectfulTransformDescriptor;
use obzenflow_runtime::stages::resources_builder::DirectFactPlan;

#[path = "support/typed_effectful.rs"]
mod support;
use support::FirstOnly;

fn main() {
    let _ = EffectfulTransformDescriptor {
        name: "counterfeit-generated-stage".to_string(),
        handler: FirstOnly,
        effects: Vec::new(),
        observers: Vec::new(),
        effect_policies: Vec::new(),
        direct_fact_plan: DirectFactPlan::default(),
        pass_through_event_type: None,
        backpressure: None,
    };
}
