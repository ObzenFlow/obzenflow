use async_trait::async_trait;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;

#[path = "support/typed_effectful.rs"]
mod support;
use support::Input;

#[derive(Clone, Debug)]
struct MissingContractTypes;

#[async_trait]
impl EffectfulTransformHandler for MissingContractTypes {
    type Input = Input;

    async fn process(
        &self,
        _input: Self::Input,
        _fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        unimplemented!()
    }
}

fn main() {}
