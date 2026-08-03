use obzenflow::ai::{
    ChatCompletion, ChatEffectBinding, ChatResponseFormat, ChatTransformBuilder,
    EmbeddingDimensions, EmbeddingEffectBinding, EmbeddingGeneration,
    EmbeddingTransformBuilder, ToolDefinition,
};
use obzenflow_adapters::middleware::control::ai_resilience;
use obzenflow_core::config::SecretRef;
use obzenflow_core::http_client::Url;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::effectful_transform;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Input {
    text: String,
}

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "trybuild.standalone.input";
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatOutput {
    text: String,
}

impl TypedPayload for ChatOutput {
    const EVENT_TYPE: &'static str = "trybuild.standalone.chat_output";
}

#[derive(Debug, Serialize, Deserialize)]
struct EmbeddingOutput {
    vectors: Vec<Vec<f32>>,
}

impl TypedPayload for EmbeddingOutput {
    const EVENT_TYPE: &'static str = "trybuild.standalone.embedding_output";
}

fn main() {
    let secret = SecretRef::new("TRYBUILD_AI_API_KEY");
    let compatible = Url::parse("http://localhost:1234/v1").unwrap();
    let _ = ChatEffectBinding::openai("model", secret.clone()).unwrap();
    let _ = ChatEffectBinding::openai_compatible(
        "model",
        secret.clone(),
        compatible.clone(),
    )
    .unwrap();
    let _ = EmbeddingEffectBinding::openai("embedding-model", secret.clone()).unwrap();
    let _ = EmbeddingEffectBinding::openai_compatible(
        "embedding-model",
        secret,
        compatible,
    )
    .unwrap();

    let (chat, _chat_registration) = ChatEffectBinding::ollama("model", None)
        .unwrap()
        .into_parts();
    let chat_handler = ChatTransformBuilder::from_binding(chat)
        .logic_version("chat-v1")
        .system("Be concise")
        .temperature(0.2)
        .max_tokens(100)
        .top_p(0.9)
        .seed(7)
        .response_format(ChatResponseFormat::JsonObject)
        .tools(vec![ToolDefinition {
            name: "lookup_ticket".to_string(),
            description: Some("Look up a ticket".to_string()),
            parameters_schema: Some(serde_json::json!({"type": "object"})),
        }])
        .extra_param("fixture_parameter", serde_json::json!(true))
        .build_typed::<Input, ChatOutput>(
            |input| Ok(input.text.clone()),
            |_, response| Ok(ChatOutput { text: response.text }),
        )
        .unwrap();
    let _chat = effectful_transform!(
        Input -> ChatOutput => chat_handler,
        effects: [at_least_once(ChatCompletion) with [ai_resilience()]],
        middleware: [],
    );

    let (embedding, _embedding_registration) =
        EmbeddingEffectBinding::ollama("embedding-model", None)
            .unwrap()
            .into_parts();
    let embedding_handler = EmbeddingTransformBuilder::from_binding(embedding)
        .logic_version("embedding-v1")
        .dimensions(EmbeddingDimensions::try_from(3).unwrap())
        .build_typed::<ChatOutput, EmbeddingOutput>(
            |input| Ok(vec![input.text.clone()]),
            |_, response| Ok(EmbeddingOutput { vectors: response.vectors }),
        )
        .unwrap();
    let _embedding = effectful_transform!(
        ChatOutput -> EmbeddingOutput => embedding_handler,
        effects: [at_least_once(EmbeddingGeneration) with [ai_resilience()]],
        middleware: [],
    );
}
