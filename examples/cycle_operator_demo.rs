//! Demo: The <| operator for cycles (FLOWIP-082)
//! 
//! This demonstrates how the <| operator creates backward edges in the topology,
//! enabling retry/fix patterns where failed items can be repaired and reprocessed.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde_json::json;

/// Source that generates JSON documents - some valid, some with missing fields
#[derive(Clone, Debug)]
struct DocumentGenerator {
    count: usize,
    writer_id: WriterId,
}

impl DocumentGenerator {
    fn new() -> Self {
        Self {
            count: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for DocumentGenerator {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= 5 {
            return None;
        }

        self.count += 1;

        // Generate documents - some missing required "title" field
        let doc = match self.count {
            1 => json!({ "id": 1, "title": "Valid Document", "content": "Has all fields" }),
            2 => json!({ "id": 2, "content": "Missing title field!" }), // Missing title!
            3 => json!({ "id": 3, "title": "Another Valid One", "content": "Complete" }),
            4 => json!({ "id": 4, "content": "Also missing title" }), // Missing title!
            5 => json!({ "id": 5, "title": "Last Valid Document", "content": "All good" }),
            _ => unreachable!(),
        };

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "document",
            doc,
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= 5
    }
}

/// Validator that checks for required fields
#[derive(Clone, Debug)]
struct Validator;

#[async_trait]
impl TransformHandler for Validator {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let doc = event.payload();
        let id = doc["id"].as_i64().unwrap_or(0);
        
        // Check if document has required "title" field
        if doc.get("title").is_some() {
            println!("✅ Document {} is valid - has title: \"{}\"", id, doc["title"]);
            
            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "document.valid",
                doc,
            )]
        } else {
            println!("❌ Document {} is invalid - missing title field", id);
            
            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "document.invalid",
                doc,
            )]
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Fixer that adds missing fields
#[derive(Clone, Debug)]
struct Fixer;

#[async_trait]
impl TransformHandler for Fixer {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Only process invalid documents!
        if event.event_type() != "document.invalid" {
            return vec![];  // Ignore non-invalid documents
        }
        
        let mut doc = event.payload();
        let id = doc["id"].as_i64().unwrap_or(0);
        
        // Add missing title field
        doc["title"] = json!(format!("Fixed Document {}", id));
        println!("🔧 Fixed document {} - added title: \"{}\"", id, doc["title"]);
        
        // Send it back as a regular document so validator processes it normally
        vec![ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            "document",  // Same type as original!
            doc,
        )]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Storage sink for valid documents
#[derive(Clone, Debug)]
struct Storage {
    name: String,
}

impl Storage {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl SinkHandler for Storage {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, Box<dyn std::error::Error + Send + Sync>> {
        let doc = event.payload();
        let id = doc["id"].as_i64().unwrap_or(0);
        
        match event.event_type().as_str() {
            "document.valid" => {
                println!("💾 {} stored document {}: \"{}\"", self.name, id, doc["title"]);
            }
            _ => {
                // Ignore other event types
            }
        }
        
        Ok(DeliveryPayload::success(
            &self.name,
            DeliveryMethod::Custom("Storage".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use console exporter
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");
    
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=info")
        .with_target(false)
        .with_thread_ids(false)
        .init();

    println!("🔄 Cycle Operator Demo - Document Validation with Retry");
    println!("======================================================\n");

    let journal_path = std::path::PathBuf::from("target/cycle_operator_demo_journal");

    // Create flow demonstrating the <| operator
    let flow_handle = flow! {
        name: "document_processor",
        journals: disk_journals(journal_path.clone()),
        middleware: [],

        stages: {
            source = source!("source" => DocumentGenerator::new());
            validator = transform!("validator" => Validator);
            fixer = transform!("fixer" => Fixer);
            storage = sink!("storage" => Storage::new("DocumentStore"));
        },

        topology: {
            // Main flow: source -> validator -> storage
            source |> validator;
            validator |> storage;
            
            // Retry flow: invalid docs go to fixer, then back to validator
            validator |> fixer;      // Invalid docs to fixer
            validator <| fixer;      // THE CYCLE: fixed docs back to validator!
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    println!("📋 Processing 5 documents (2 are missing required fields)");
    println!("🔄 Invalid documents will be fixed and retried\n");

    // Run the flow to completion
    let metrics_exporter = flow_handle.run_with_metrics().await?;

    println!("\n📊 Flow Metrics Summary:");
    println!("========================");
    
    // Get the metrics summary after completion
    if let Some(exporter) = metrics_exporter {
        let summary = exporter
            .render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
        println!("{}", summary);
    }

    println!("\n✅ Flow completed successfully!");
    println!("\n🔍 What happened:");
    println!("- Documents 1, 3, 5 were valid (had title field)");
    println!("- Documents 2, 4 were invalid (missing title)");
    println!("- Fixer added missing titles to documents 2, 4");
    println!("- Fixed documents cycled back to validator via <|");
    println!("- All 5 documents eventually stored successfully");

    Ok(())
}