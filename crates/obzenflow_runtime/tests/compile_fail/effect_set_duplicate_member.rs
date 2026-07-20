// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// Duplicate members in an explicit effect capability set are compile
// errors: the membership index would be ambiguous and the declaration lies
// about the set's cardinality (FLOWIP-120z).

use async_trait::async_trait;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_runtime::effect_set;
use obzenflow_runtime::effects::{
    assert_distinct_effect_set, Effect, EffectContext, EffectError, EffectSafety,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug)]
struct PingEffect;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PingOutput;

impl TypedPayload for PingOutput {
    const EVENT_TYPE: &'static str = "test.ping.output";
}

#[async_trait]
impl Effect for PingEffect {
    const EFFECT_TYPE: &'static str = "test.ping";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = PingOutput;

    fn label(&self) -> &str {
        "ping"
    }

    fn canonical_input(&self) -> Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(PingOutput)
    }
}

const _: () = assert_distinct_effect_set::<effect_set![PingEffect, PingEffect]>();

fn main() {}
