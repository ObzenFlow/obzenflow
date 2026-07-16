use obzenflow_core::{StageOutputFacts, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TypedTransformHandler;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.transform.carrier_mismatch.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct First;
impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.transform.carrier_mismatch.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Second;
impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.transform.carrier_mismatch.second";
}

#[derive(Clone, Debug, StageOutputFacts)]
enum DeclaredOutput {
    First(First),
    Second(Second),
}

#[derive(Clone, Debug, StageOutputFacts)]
enum HandlerOutput {
    First(First),
    Second(Second),
}

#[derive(Clone, Debug)]
struct Handler;
impl TypedTransformHandler for Handler {
    type Input = Input;
    type Output = HandlerOutput;

    fn process(&self, _input: Input) -> Result<Self::Output, HandlerError> {
        Ok(HandlerOutput::First(First))
    }
}

fn main() {
    let _ = obzenflow_dsl::transform!(
        Input -> DeclaredOutput,
        outputs: [First, Second] => Handler
    );
}
