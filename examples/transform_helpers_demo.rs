//! Example demonstrating transform helper types (FLOWIP-080h)
//!
//! This example shows how to use the Filter, Map, and TryMapWith helpers
//! to create clean, composable transform stages with flexible error handling.

use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, TransformHandler};
use obzenflow_runtime_services::stages::transform::{Filter, Map, TryMapWith};
use obzenflow_core::{ChainEvent, WriterId};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::id::StageId;
use serde_json::json;

// Simple source that generates test events
#[derive(Clone, Debug)]
struct TestEventSource {
    remaining: usize,
    writer_id: WriterId,
}

impl TestEventSource {
    fn new(count: usize) -> Self {
        Self {
            remaining: count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TestEventSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        // Generate events with different log levels and values
        let level = if self.remaining % 3 == 0 { "error" } else { "info" };
        let value = (self.remaining * 10) as f64;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "log_event",
            json!({
                "level": level,
                "value": value,
                "message": format!("Event {}", self.remaining),
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.remaining == 0
    }
}

fn main() {
    println!("Transform Helpers Demo (FLOWIP-080h)\n");
    println!("This example demonstrates transform helper types:");
    println!("  - Filter: predicate-based filtering");
    println!("  - Map: 1-to-1 transformations (always succeeds)");
    println!("  - TryMapWith: fallible transformations with validation\n");
    println!("Map is the primitive. TryMap/TryMapWith are fallible variants.\n");

    // 1. Filter Example - only pass error events
    println!("=== 1. Filter Helper ===");
    let error_filter = Filter::new(|event| {
        event.payload()["level"].as_str() == Some("error")
    });
    println!("Created filter that only passes 'error' level events\n");

    // 2. Map Example - add processed field
    println!("=== 2. Map Helper ===");
    let add_processed_flag = Map::new(|event| {
        let mut payload = event.payload();
        payload["processed"] = json!(true);

        ChainEventFactory::data_event(
            event.writer_id.clone(),
            event.event_type(),
            payload,
        )
    });
    println!("Created mapper that adds processed flag\n");

    // 3. TryMapWith Example - validate and extract value
    println!("=== 3. TryMapWith Helper (Fallible Map) ===");
    let validate_value = TryMapWith::new(|event| {
        let value = event.payload()["value"]
            .as_f64()
            .ok_or_else(|| "Missing value field".to_string())?;

        if value < 0.0 {
            return Err("Negative values not allowed".to_string());
        }

        if value > 100.0 {
            return Err("Value too large (max 100)".to_string());
        }

        // Valid - create new event with validated flag
        let mut payload = event.payload();
        payload["validated"] = json!(true);

        Ok(ChainEventFactory::data_event(
            event.writer_id.clone(),
            "validated_event",
            payload,
        ))
    });
    println!("Created converter that validates value range (0-100)\n");

    // 4. Demonstrate processing
    println!("=== 4. Processing Events ===");
    let mut source = TestEventSource::new(10);

    println!("Generating 10 events and processing through helpers:\n");

    while let Some(event) = source.next() {
        let payload = event.payload();
        let level = payload["level"].as_str().unwrap_or("unknown");
        let value = payload["value"].as_f64().unwrap_or(0.0);

        println!("Original event: level={}, value={}", level, value);

        // Apply filter
        let filtered = error_filter.process(event.clone());
        if filtered.is_empty() {
            println!("  -> Filtered out (not an error)");
            continue;
        }
        println!("  -> Passed filter (is error)");

        // Apply map
        let mapped = add_processed_flag.process(filtered[0].clone());
        println!("  -> Mapped (added processed flag)");

        // Apply conversion with validation
        let converted = validate_value.process(mapped[0].clone());
        if let Some(result) = converted.first() {
            use obzenflow_core::event::status::processing_status::ProcessingStatus;
            match &result.processing_info.status {
                ProcessingStatus::Error(msg) => {
                    println!("  -> Conversion failed: {}", msg);
                }
                _ => {
                    println!("  -> Validated successfully");
                }
            }
        }

        println!();
    }

    // 5. Demonstrate Composable Error Handling
    println!("=== 5. Composable Error Handling Strategies ===\n");

    // Create a simple event for testing
    let test_event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test",
        json!({"amount": -50.0}),  // Invalid amount
    );

    // Strategy 1: Error Journal (default)
    println!("Strategy 1: ToErrorJournal (default)");
    let validator_journal = TryMapWith::new(|event| {
        let amount = event.payload()["amount"].as_f64().ok_or("Missing amount")?;
        if amount < 0.0 { return Err("Negative amount".to_string()); }
        Ok(event)
    })
    .on_error_journal();  // Explicit (this is the default)

    let result = validator_journal.process(test_event.clone());
    println!("  → Events marked with ProcessingStatus::Error go to error journal");
    println!("  → Use for: Terminal errors that should be logged\n");

    // Strategy 2: Emit as Custom Event Type
    println!("Strategy 2: ToEventType (for cycle-based error handling)");
    let validator_emit = TryMapWith::new(|event| {
        let amount = event.payload()["amount"].as_f64().ok_or("Missing amount")?;
        if amount < 0.0 { return Err("Negative amount".to_string()); }
        Ok(event)
    })
    .on_error_emit("Transaction.invalid");  // ✨ Composable!

    let result = validator_emit.process(test_event.clone());
    println!("  → Failed validations emit as 'Transaction.invalid' events");
    println!("  → Event type: {}", result[0].event_type());
    println!("  → Payload includes validation_error field");
    println!("  → Use for: Fix/retry cycles, recoverable errors\n");

    // Strategy 3: Drop Errors
    println!("Strategy 3: Drop (silent failure)");
    let validator_drop = TryMapWith::new(|event| {
        let amount = event.payload()["amount"].as_f64().ok_or("Missing amount")?;
        if amount < 0.0 { return Err("Negative amount".to_string()); }
        Ok(event)
    })
    .on_error_drop();

    let result = validator_drop.process(test_event.clone());
    println!("  → Failed validations produce no output (dropped)");
    println!("  → Result: {} events emitted", result.len());
    println!("  → Use for: Expected failures, filtering patterns\n");

    // Strategy 4: Custom Handler
    println!("Strategy 4: Custom (full control)");
    let validator_custom = TryMapWith::new(|event| {
        let amount = event.payload()["amount"].as_f64().ok_or("Missing amount")?;
        if amount < 0.0 { return Err("Negative amount".to_string()); }
        Ok(event)
    })
    .on_error_with(|event, error| {
        println!("  → Custom handler called with error: {}", error);
        let mut payload = event.payload();
        payload["error"] = json!(error);
        payload["retry_count"] = json!(1);

        Some(ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            "Transaction.failed",
            payload,
        ))
    });

    let result = validator_custom.process(test_event.clone());
    println!("  → Custom event created with retry logic");
    println!("  → Event type: {}", result[0].event_type());
    println!("  → Use for: Complex error handling, circuit breakers\n");

    println!("=== Summary ===");
    println!("Transform helpers provide clean, composable patterns for:");
    println!("  ✓ Filtering events by predicates (Filter)");
    println!("  ✓ Transforming event payloads (Map - always succeeds)");
    println!("  ✓ Fallible transformations (TryMap/TryMapWith - can fail)");
    println!("\nComposable Error Handling:");
    println!("  ✓ .on_error_journal() - Route to error journal (terminal errors)");
    println!("  ✓ .on_error_emit(type) - Emit as custom event (cycle-based handling)");
    println!("  ✓ .on_error_drop() - Silently drop failures (filtering)");
    println!("  ✓ .on_error_with(fn) - Custom error handling logic");
    println!("\nMap is the primitive. TryMap/TryMapWith are fallible variants.");
    println!("Error strategies make TryMap/TryMapWith truly composable!");
    println!("See FLOWIP-080h for full documentation.");
}
