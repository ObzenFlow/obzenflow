use async_trait::async_trait;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulStatefulHandler;

#[path = "support/typed_effectful.rs"]
mod support;
use support::Input;

#[derive(Clone, Debug)]
struct MissingContractTypes;

#[async_trait]
impl EffectfulStatefulHandler for MissingContractTypes {
    type State = ();
    type Input = Input;

    fn initial_state(&self) -> Self::State {}

    async fn decide(
        &mut self,
        _state: &Self::State,
        _input: &Self::Input,
        _fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        unimplemented!()
    }

    fn apply(
        &mut self,
        _state: &mut Self::State,
        _fact: Self::Output,
    ) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn main() {}
