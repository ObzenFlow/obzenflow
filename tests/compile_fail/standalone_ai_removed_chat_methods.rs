mod support {
    include!("support/ai_surface.rs");
}

use obzenflow::ai::{ChatResponse, ChatTransform, ChatTransformBuilder};
use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use support::{Input, Output};

fn builder() -> ChatTransformBuilder {
    ChatTransformBuilder::from_binding(support::contract())
}

fn main() {
    let _ = ChatTransformBuilder::new();
    let _: ChatTransformBuilder = Default::default();
    let _ = builder().ollama("model");
    let _ = builder().openai("model", "secret");
    let _ = builder().openai_compatible("model", "secret", "http://localhost/v1");
    let _ = builder().base_url("http://localhost");
    let _ = builder().provider_label();
    let _ = builder().model_label();
    let _ = builder().context(());
    let _ = builder().output_mapper(
        |_: &ChainEvent, _: ChatResponse| -> Result<Vec<ChainEvent>, HandlerError> {
            unreachable!()
        },
    );
    let _ = builder().build(|_: &ChainEvent| Ok(String::new()));
    let _ = builder().build_lazy(|_: &ChainEvent| Ok(String::new()));
    let _ = builder().build_messages(|_: &ChainEvent| Ok(Vec::new()));
    let _ = builder().build_messages_lazy(|_: &ChainEvent| Ok(Vec::new()));
    let _ = builder().build_request(|_: &ChainEvent| unreachable!());
    let _ = builder().build_request_lazy(|_: &ChainEvent| unreachable!());
    let _ = builder().build_typed_lazy::<Input, Output>(
        |_| Ok(String::new()),
        |_, _| Ok(Output),
    );
    let transform = builder()
        .logic_version("v1")
        .build_typed::<Input, Output>(|_| Ok(String::new()), |_, _| Ok(Output))
        .unwrap();
    let _ = transform.with_output_mapper(
        |_: &ChainEvent, _: ChatResponse| -> Result<Vec<ChainEvent>, HandlerError> {
            unreachable!()
        },
    );
    let transform = builder()
        .logic_version("v1")
        .build_typed::<Input, Output>(|_| Ok(String::new()), |_, _| Ok(Output))
        .unwrap();
    let _ = transform.with_lineage_aware_output_mapper(
        |_: &ChainEvent, _: ChatResponse, _| -> Result<Vec<ChainEvent>, HandlerError> {
            unreachable!()
        },
    );
    let _ = ChatTransform::<Input, Output>::builder();
    let _ = ChatTransform::<Input, Output>::new((), ());
}
