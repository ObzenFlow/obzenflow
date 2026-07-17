use async_trait::async_trait;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TransformHandler;
use obzenflow_runtime::typing::TransformTyping;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.raw_transform.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct First;

impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.raw_transform.first";
}

#[derive(Clone, Debug)]
struct RawTransform;

impl TransformTyping for RawTransform {
    type Input = Input;
    type Output = First;
}

#[async_trait]
impl TransformHandler for RawTransform {
    fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn main() {
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> First => RawTransform,
        effects: [],
        middleware: []
    );
}
