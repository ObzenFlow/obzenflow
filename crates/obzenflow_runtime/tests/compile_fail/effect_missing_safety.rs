// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug)]
struct MissingSafetyEffect;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MissingSafetyOutput;

impl TypedPayload for MissingSafetyOutput {
    const EVENT_TYPE: &'static str = "test.missing_safety.output";
}

#[async_trait]
impl Effect for MissingSafetyEffect {
    const EFFECT_TYPE: &'static str = "test.missing_safety";
    const SCHEMA_VERSION: u32 = 1;

    type Output = MissingSafetyOutput;

    fn label(&self) -> &str {
        "missing_safety"
    }

    fn canonical_input(&self) -> Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        Ok(MissingSafetyOutput)
    }
}

fn main() {}
