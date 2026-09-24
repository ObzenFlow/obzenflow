// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal construction and inspection for application runs.

/// Consumer handles for read-only snapshots, live tails and journal-derived progress.
pub mod read {
    pub use obzenflow_core::event::context::StageType;
    pub use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
    pub use obzenflow_core::event::payloads::execution_payload::{
        ExecutionPayload, StageLifecycleFact,
    };
    pub use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
    pub use obzenflow_core::event::payloads::supervisor_descriptor::{
        SupervisionMode, SupervisorDescriptor, SupervisorKind,
    };
    pub use obzenflow_core::event::{ChainPayload, PipelineLifecycleEvent, SystemPayload};
    pub use obzenflow_core::journal::read::*;
    pub use obzenflow_core::journal::{RunManifest, RUN_MANIFEST_FILENAME};
    pub use obzenflow_infra::journal::read::{
        open_disk_run, JournalReadError, RunSnapshot, RunTail,
    };
}

pub use obzenflow_core::journal::JOURNAL_SCHEMA_VERSION;
pub use obzenflow_core::journal::{Journal, JournalError};
pub use obzenflow_core::{JournalOwner, JournalRecord};
pub use obzenflow_infra::journal::disk::inspect::{export_jsonl, inspect, JournalInspectError};
pub use obzenflow_infra::journal::{
    disk_journals, memory_journals, DiskJournal, DiskJournalFactory, MemoryJournal,
    MemoryJournalFactory,
};

// Read-only evidence vocabulary used when inspecting recorded runs.
pub use obzenflow_core::event::payloads::effect_payload::{
    EffectOutcomePayload, EffectRecord, EFFECT_RECORD_EVENT_TYPE,
};
pub use obzenflow_core::event::status::processing_status::ProcessingStatus;
pub use obzenflow_core::event::{ChainPayload, StageFatalRecorded};
