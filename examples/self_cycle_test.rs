//! Test that self-cycles are properly rejected
//! This test validates FLOWIP-082's prohibition of self-cycles

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, transform};
use obzenflow_infra::journal::disk_journals;
use std::path::PathBuf;
use obzenflow_runtime_services::stages::common::handlers::TransformHandler;

#[derive(Clone, Debug)]
struct TestProcessor;

#[async_trait]
impl TransformHandler for TestProcessor {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Testing Self-Cycle Rejection ===");
    
    // This SHOULD FAIL - self-cycles are forbidden
    let flow_result = flow! {
        name: "self_cycle_test",
        journals: disk_journals(PathBuf::from("/tmp/test_journals")),
        middleware: [],
        
        stages: {
            processor = transform!("test_processor" => TestProcessor);
        },
        
        topology: {
            // This creates a self-cycle, which should be rejected
            processor <| processor;
        }
    }.await;
    
    match flow_result {
        Ok(_) => {
            eprintln!("❌ ERROR: Self-cycle was accepted but should have been rejected!");
            std::process::exit(1);
        }
        Err(e) => {
            println!("✅ Self-cycle properly rejected with error: {}", e);
            
            // Verify it's the right error
            if e.to_string().contains("SelfCycle") {
                println!("✅ Correct error type (SelfCycle)");
                Ok(())
            } else {
                eprintln!("❌ Wrong error type: {}", e);
                std::process::exit(1);
            }
        }
    }
}