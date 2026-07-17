use async_trait::async_trait;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::StatefulHandler;
use obzenflow_runtime::typing::StatefulTyping;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.raw_stateful.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct First;

impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.raw_stateful.first";
}

#[derive(Clone, Debug)]
struct RawStateful;

impl StatefulTyping for RawStateful {
    type Input = Input;
    type Output = First;
}

#[async_trait]
impl StatefulHandler for RawStateful {
    type State = ();

    fn accumulate(&mut self, _state: &mut Self::State, _event: ChainEvent) {}

    fn initial_state(&self) -> Self::State {}

    fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

fn main() {
    let _ = obzenflow_dsl::effectful_stateful!(
        Input -> First => RawStateful,
        effects: [],
        middleware: []
    );
}
