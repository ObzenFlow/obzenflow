use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, Effects};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Outcome;
impl TypedPayload for Outcome {
    const EVENT_TYPE: &'static str = "compile_fail.effects.perform.outcome";
}

#[derive(Clone, Debug)]
struct UndeclaredEffect;

#[async_trait]
impl Effect for UndeclaredEffect {
    const EFFECT_TYPE: &'static str = "compile_fail.effects.perform.undeclared";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type Outcome = Outcome;

    fn label(&self) -> &str { "undeclared" }
    fn canonical_input(&self) -> serde_json::Value { serde_json::Value::Null }
    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Outcome, EffectError> {
        Ok(Outcome)
    }
}

async fn perform_outside_capability(
    fx: &mut Effects<Outcome, obzenflow_runtime::effect_set![]>,
) {
    fx.perform(UndeclaredEffect).await.unwrap();
}

fn main() {}
