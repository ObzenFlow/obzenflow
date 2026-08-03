use obzenflow::ai::ModelConfig;

fn main() {
    let config = ModelConfig::ollama("model");
    let _ = config.chat_builder();
    let _ = config.chat();
}
