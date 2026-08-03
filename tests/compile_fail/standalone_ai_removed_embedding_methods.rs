mod support {
    include!("support/ai_surface.rs");
}

use obzenflow::ai::{
    EmbeddingBindingContract, EmbeddingResponse, EmbeddingTarget, EmbeddingTransform,
    EmbeddingTransformBuilder,
};
use obzenflow_core::ai::{embedding_binding_fingerprint, AiProvider};
use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use support::{Input, Output};

fn builder() -> EmbeddingTransformBuilder {
    let provider = AiProvider::new("fixture");
    let target = EmbeddingTarget::new(
        provider.clone(),
        "model",
        embedding_binding_fingerprint(&provider, "model", "http://fixture.invalid"),
    );
    EmbeddingTransformBuilder::from_binding(EmbeddingBindingContract::from_target(target))
}

fn main() {
    let _ = EmbeddingTransformBuilder::new();
    let _: EmbeddingTransformBuilder = Default::default();
    let _ = builder().ollama("model");
    let _ = builder().openai("model", "secret");
    let _ = builder().openai_compatible("model", "secret", "http://localhost/v1");
    let _ = builder().base_url("http://localhost");
    let _ = builder().provider_label();
    let _ = builder().model_label();
    let _ = builder().output_mapper(
        |_: &ChainEvent, _: EmbeddingResponse| -> Result<Vec<ChainEvent>, HandlerError> {
            unreachable!()
        },
    );
    let _ = builder().build(|_: &ChainEvent| Ok(Vec::new()));
    let _ = builder().build_lazy(|_: &ChainEvent| Ok(Vec::new()));
    let transform = builder()
        .logic_version("v1")
        .build_typed::<Input, Output>(|_| Ok(Vec::new()), |_, _| Ok(Output))
        .unwrap();
    let _ = transform.with_output_mapper(
        |_: &ChainEvent, _: EmbeddingResponse| -> Result<Vec<ChainEvent>, HandlerError> {
            unreachable!()
        },
    );
    let _ = EmbeddingTransform::<Input, Output>::builder();
    let _ = EmbeddingTransform::<Input, Output>::new((), ());
}
