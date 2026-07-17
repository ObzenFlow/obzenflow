use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, FirstOnly};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OtherInput;

impl TypedPayload for OtherInput {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.other_transform_input";
}

fn main() {
    let _ = obzenflow_dsl::effectful_transform!(
        OtherInput -> First => FirstOnly,
        effects: [],
        middleware: []
    );
}
