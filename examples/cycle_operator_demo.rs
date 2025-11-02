//! Demo: Feedback Loops with the <| Backflow Operator (FLOWIP-082)
//!
//! This demonstrates how the <| operator creates backward edges in the topology,
//! enabling validation/fix/retry patterns where failed items can be repaired
//! and reprocessed through the pipeline.
//!
//! **Reference Example for**: AI/LLM feedback loops, retry patterns, stateful validation
//!
//! Key concepts demonstrated:
//! - Backflow operator (<|) for creating cycles
//! - StatefulHandler for tracking retry attempts per document
//! - CycleGuardMiddleware for automatic infinite loop protection
//! - FlowApplication for modern pipeline management
//! - Real-world validation/fix/retry pattern

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler, TransformHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde_json::json;
use std::collections::HashMap;

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
            println!("[VALID] Document {} has title: \"{}\"", id, doc["title"]);

            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "document.valid",
                doc,
            )]
        } else {
            println!("[INVALID] Document {} missing title field", id);

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

/// State tracking retry attempts for each document
#[derive(Clone, Debug, Default)]
struct FixerState {
    /// Track retry count per document ID
    retry_counts: HashMap<i64, usize>,
    /// Maximum retries before giving up
    max_retries: usize,
}

/// Stateful fixer that adds missing fields and tracks retry attempts
///
/// This demonstrates how StatefulHandler can track per-item state across
/// feedback loop iterations - critical for AI/LLM use cases where you need
/// to limit retry attempts and track improvement progress.
#[derive(Clone, Debug)]
struct StatefulFixer {
    writer_id: WriterId,
}

impl StatefulFixer {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for StatefulFixer {
    type State = FixerState;

    fn process(&self, state: &Self::State, event: ChainEvent) -> (Self::State, Vec<ChainEvent>) {
        // Only process invalid documents!
        if event.event_type() != "document.invalid" {
            return (state.clone(), vec![]);  // Ignore non-invalid documents
        }

        let mut doc = event.payload();
        let id = doc["id"].as_i64().unwrap_or(0);

        let mut new_state = state.clone();
        let retry_count = new_state.retry_counts.get(&id).copied().unwrap_or(0);

        // Check if we've exceeded max retries
        if retry_count >= new_state.max_retries {
            println!("[FAILED] Document {} exceeded max retries ({}), giving up", id, new_state.max_retries);

            // Emit a failure event instead of retrying
            let failure_event = ChainEventFactory::derived_data_event(
                self.writer_id.clone(),
                &event,
                "document.failed",
                json!({
                    "id": id,
                    "reason": "exceeded_max_retries",
                    "retry_count": retry_count,
                }),
            );

            return (new_state, vec![failure_event]);
        }

        // Increment retry count
        new_state.retry_counts.insert(id, retry_count + 1);

        // Add missing title field
        doc["title"] = json!(format!("Fixed Document {}", id));
        println!("[FIX] Document {} - added title: \"{}\" (retry attempt {})",
                 id, doc["title"], retry_count + 1);

        // Send it back as a regular document so validator processes it normally
        let fixed_event = ChainEventFactory::derived_data_event(
            self.writer_id.clone(),
            &event,
            "document",  // Same type as original - will cycle back!
            doc,
        );

        (new_state, vec![fixed_event])
    }

    fn initial_state(&self) -> Self::State {
        FixerState {
            retry_counts: HashMap::new(),
            max_retries: 3,  // Allow up to 3 retry attempts
        }
    }

    async fn drain(
        &mut self,
        state: &Self::State,
    ) -> Result<Vec<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
        // Log final retry statistics
        if !state.retry_counts.is_empty() {
            println!("\n[STATS] Retry attempts per document:");
            let mut ids: Vec<_> = state.retry_counts.keys().collect();
            ids.sort();
            for id in ids {
                let count = state.retry_counts.get(id).unwrap();
                println!("  Document {}: {} attempt(s)", id, count);
            }
        }
        Ok(vec![])
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
                println!("[STORED] {} saved document {}: \"{}\"", self.name, id, doc["title"]);
            }
            "document.failed" => {
                let reason = doc["reason"].as_str().unwrap_or("unknown");
                println!("[REJECTED] {} rejected document {} (reason: {})", self.name, id, reason);
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

    println!("Feedback Loop Demo - Document Validation with Retry");
    println!("===================================================\n");
    println!("Demonstrating:");
    println!("  • Backflow operator (<|) for creating cycles");
    println!("  • StatefulHandler for tracking retry attempts");
    println!("  • Automatic cycle protection (CycleGuardMiddleware added by framework)");
    println!("  • Real-world validation/fix/retry pattern\n");

    let journal_path = std::path::PathBuf::from("target/cycle_operator_demo_journal");

    // Use FlowApplication for modern pattern
    FlowApplication::run(async {
        flow! {
            name: "document_processor",
            journals: disk_journals(journal_path.clone()),
            middleware: [],  // CycleGuardMiddleware is added automatically for topologies with cycles

            stages: {
                source = source!("source" => DocumentGenerator::new());
                validator = transform!("validator" => Validator);
                fixer = stateful!("fixer" => StatefulFixer::new());
                storage = sink!("storage" => Storage::new("DocumentStore"));
            },

            topology: {
                // Main flow: source -> validator -> storage
                source |> validator;
                validator |> storage;

                // Feedback loop: invalid docs go to fixer, then back to validator
                validator |> fixer;      // Invalid docs to fixer
                validator <| fixer;      // THE CYCLE: fixed docs back to validator!
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("\n\nFlow completed successfully!");
    println!("\nWhat happened:");
    println!("  • Documents 1, 3, 5 were valid (had title field)");
    println!("  • Documents 2, 4 were invalid (missing title)");
    println!("  • Fixer added missing titles to documents 2, 4");
    println!("  • Fixed documents cycled back to validator via <|");
    println!("  • All 5 documents eventually stored successfully");
    println!("\nKey patterns for AI/LLM feedback loops:");
    println!("  • StatefulHandler tracks retry attempts per item");
    println!("  • Max retry limit prevents infinite improvement loops");
    println!("  • Framework automatically adds cycle protection for topologies with backflow");
    println!("  • Backflow operator (<|) enables feedback/refinement cycles");

    Ok(())
}
