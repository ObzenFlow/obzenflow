use async_trait::async_trait;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError};
use serde_json::{json, Value};

#[derive(Clone, Debug)]
struct MissingSafetyEffect;

#[async_trait]
impl Effect for MissingSafetyEffect {
    const EFFECT_TYPE: &'static str = "test.missing_safety";
    const SCHEMA_VERSION: u32 = 1;

    type Output = ();

    fn label(&self) -> &str {
        "missing_safety"
    }

    fn canonical_input(&self) -> Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        Ok(())
    }
}

fn main() {}
