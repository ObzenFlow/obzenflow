use obzenflow_core::{StageOutputFacts, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TypedTransformHandler;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Input;
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.transform.brace_carrier.input";
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct First;
impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.transform.brace_carrier.first";
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Second;
impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.transform.brace_carrier.second";
}

#[derive(StageOutputFacts)]
enum Output {
    First(First),
    Second(Second),
}

#[derive(Clone, Debug)]
struct Handler;

impl TypedTransformHandler for Handler {
    type Input = Input;
    type Output = Output;

    fn process(&self, _input: Self::Input) -> Result<Self::Output, HandlerError> {
        Ok(Output::First(First))
    }
}

fn main() {
    // A typed handler whose Output is `Output` must name that carrier explicitly.
    let _ = obzenflow_dsl::transform!(Input -> { First, Second } => Handler);
}
