use async_trait::async_trait;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TransformHandler;
use obzenflow_runtime::typing::TransformTyping;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.transform.raw.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct First;
impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.transform.raw.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Second;
impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.transform.raw.second";
}

#[derive(Clone, Debug)]
struct RawHandler;

impl TransformTyping for RawHandler {
    type Input = Input;
    type Output = First;
}

#[async_trait]
impl TransformHandler for RawHandler {
    fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn main() {
    let _ = obzenflow_dsl::transform!(Input -> { First, Second } => RawHandler);
}
