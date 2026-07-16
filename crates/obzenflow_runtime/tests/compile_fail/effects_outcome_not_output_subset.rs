use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, Effects};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StageOutput;
impl TypedPayload for StageOutput {
    const EVENT_TYPE: &'static str = "compile_fail.effects.subset.stage_output";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EffectOutcome;
impl TypedPayload for EffectOutcome {
    const EVENT_TYPE: &'static str = "compile_fail.effects.subset.effect_outcome";
}

#[derive(Clone, Debug)]
struct AllowedEffect;

#[async_trait]
impl Effect for AllowedEffect {
    const EFFECT_TYPE: &'static str = "compile_fail.effects.subset.allowed";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type Outcome = EffectOutcome;

    fn label(&self) -> &str { "allowed" }
    fn canonical_input(&self) -> serde_json::Value { serde_json::Value::Null }
    async fn execute(&self, _ctx: &mut EffectContext) -> Result<EffectOutcome, EffectError> {
        Ok(EffectOutcome)
    }
}

async fn perform_with_outcome_outside_output(
    fx: &mut Effects<StageOutput, obzenflow_runtime::effect_set![AllowedEffect]>,
) {
    fx.perform(AllowedEffect).await.unwrap();
}

fn main() {}
