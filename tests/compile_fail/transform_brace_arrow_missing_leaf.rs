use obzenflow_core::{StageOutputFacts, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TypedTransformHandler;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.transform.missing.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct First;
impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.transform.missing.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Missing;
impl TypedPayload for Missing {
    const EVENT_TYPE: &'static str = "compile_fail.transform.missing.second";
}

#[derive(Clone, Debug, StageOutputFacts)]
enum Output {
    First(First),
}

#[derive(Clone, Debug)]
struct Handler;
impl TypedTransformHandler for Handler {
    type Input = Input;
    type Output = Output;

    fn process(&self, _input: Input) -> Result<Self::Output, HandlerError> {
        Ok(Output::First(First))
    }
}

fn main() {
    let _ = obzenflow_dsl::transform!(Input -> { First, Missing } => Handler);
}
