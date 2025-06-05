// src/main.rs
use flowstate_rs::prelude::*;
use flowstate_rs::*; // brings in macros exported via #[macro_export]
use serde_json::json;
use std::path::PathBuf;

// A dummy stage that takes NewsFetched and emits ChunkIndexed
struct Chunker;
impl PipelineStep for Chunker {
    fn handle(&self, ev: ChainEvent) -> Vec<ChainEvent> {
        if ev.event_type == "NewsFetched" {
            let out = ChainEvent::new("ChunkIndexed", json!({ "info": "chunked" }));
            vec![out]
        } else {
            vec![]
        }
    }
}

// A dummy stage that takes ChunkIndexed and emits RetrievedContext
struct Rag;
impl PipelineStep for Rag {
    fn handle(&self, ev: ChainEvent) -> Vec<ChainEvent> {
        if ev.event_type == "ChunkIndexed" {
            let out = ChainEvent::new("RetrievedContext", json!({ "info": "retrieved" }));
            vec![out]
        } else {
            vec![]
        }
    }
}

// A dummy stage that takes RetrievedContext and emits ScriptGenerated
struct ScriptWriter;
impl PipelineStep for ScriptWriter {
    fn handle(&self, ev: ChainEvent) -> Vec<ChainEvent> {
        if ev.event_type == "RetrievedContext" {
            let out = ChainEvent::new("ScriptGenerated", json!({ "script": "Hello World" }));
            vec![out]
        } else {
            vec![]
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_path = PathBuf::from("./events.log");

    // Seed a NewsFetched event if none exist
    {
        let src = LogSource::new(log_path.clone(), "NewsFetched");
        let existing = src.load_events_async().await.unwrap_or_default();
        if existing.is_empty() {
            let initial = ChainEvent::new("NewsFetched", json!({ "headline": "Demo News" }));
            let snk = LogSink::new(log_path.clone());
            snk.append(&initial).await?;
        }
    }

    // Run the durable pipeline
    flow! {
        Source(source!("./events.log", "NewsFetched"))
        |> stage!(Chunker)
        |> stage!(Rag)
        |> stage!(ScriptWriter)
        |> Sink(sink!("./events.log"))
    }?;

    println!("Pipeline complete. Check events.log for output.");
    Ok(())
}
