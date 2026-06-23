// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use serde_json::json;

use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::EventEnvelope;
use obzenflow_core::journal::Journal;
use obzenflow_core::{FlowId, JournalOwner, JournalWriterId, StageId, WriterId};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime::backpressure::BackpressureWriter;
use obzenflow_runtime::effects::{
    EffectDeclaration, EffectInvocationContext, EffectPortRegistry, EffectRuntimeMode,
    SynthesizedOutcomeRegistration,
};
use obzenflow_runtime::feed_plan::StageOutputContract;
use obzenflow_runtime::messaging::upstream_subscription::StageInputPosition;

/// Test fixture for direct effect-boundary tests.
///
/// This lives in the root integration-test layer so it can use the real
/// `obzenflow_infra::journal::MemoryJournal` without adding an infra
/// dependency to lower crates.
pub struct EffectInvocationContextBuilder {
    flow_id: FlowId,
    stage_id: StageId,
    stage_key: String,
    writer_id: WriterId,
    input_seq: StageInputPosition,
    stage_logic_version: String,
    data_journal: Arc<dyn Journal<ChainEvent>>,
    parent: EventEnvelope<ChainEvent>,
    effect_runtime_mode: EffectRuntimeMode,
    effect_ports: EffectPortRegistry,
    effect_declarations: Vec<EffectDeclaration>,
    synthesized_outcomes: Vec<SynthesizedOutcomeRegistration>,
    output_contract: StageOutputContract,
    emit_enabled: bool,
}

impl EffectInvocationContextBuilder {
    pub fn new() -> Self {
        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);
        let data_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(
            MemoryJournal::<ChainEvent>::with_owner(JournalOwner::stage(stage_id)),
        );
        let parent = EventEnvelope::new(
            JournalWriterId::from(*data_journal.id()),
            ChainEventFactory::data_event(writer_id, "test.parent", json!({})),
        );

        Self {
            flow_id: FlowId::new(),
            stage_id,
            stage_key: "effect_stage".to_string(),
            writer_id,
            input_seq: StageInputPosition(1),
            stage_logic_version: "test-v1".to_string(),
            data_journal,
            parent,
            effect_runtime_mode: EffectRuntimeMode::Live,
            effect_ports: EffectPortRegistry::new(),
            effect_declarations: Vec::new(),
            synthesized_outcomes: Vec::new(),
            output_contract: StageOutputContract::empty(),
            emit_enabled: false,
        }
    }

    pub fn with_runtime_mode(mut self, mode: EffectRuntimeMode) -> Self {
        self.effect_runtime_mode = mode;
        self
    }

    pub fn with_effect_declarations(mut self, declarations: Vec<EffectDeclaration>) -> Self {
        self.effect_declarations = declarations;
        self
    }

    pub fn with_output_contract(mut self, contract: StageOutputContract) -> Self {
        self.output_contract = contract;
        self
    }

    pub fn with_emit_enabled(mut self, enabled: bool) -> Self {
        self.emit_enabled = enabled;
        self
    }

    pub fn build(self) -> EffectInvocationContext {
        EffectInvocationContext {
            flow_id: self.flow_id,
            stage_id: self.stage_id,
            stage_key: self.stage_key,
            writer_id: self.writer_id,
            input_seq: self.input_seq,
            stage_logic_version: self.stage_logic_version,
            data_journal: self.data_journal,
            flow_context: None,
            system_journal: None,
            instrumentation: None,
            heartbeat_state: None,
            parent: self.parent,
            effect_history: None,
            effect_runtime_mode: self.effect_runtime_mode,
            effect_ports: self.effect_ports,
            effect_declarations: self.effect_declarations,
            synthesized_outcomes: self.synthesized_outcomes,
            output_contract: self.output_contract,
            backpressure_writer: BackpressureWriter::disabled(),
            emit_enabled: self.emit_enabled,
            effect_boundary: None,
            observers: None,
            boundary_control_events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Default for EffectInvocationContextBuilder {
    fn default() -> Self {
        Self::new()
    }
}
