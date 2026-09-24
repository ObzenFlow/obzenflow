// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Durable identity of the state machine behind a recorded event writer.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorKind {
    Pipeline,
    MetricsAggregator,
    FiniteSource,
    AsyncFiniteSource,
    InfiniteSource,
    AsyncInfiniteSource,
    Transform,
    Stateful,
    Join,
    Sink,
}

impl SupervisorKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Pipeline => "Pipeline",
            Self::MetricsAggregator => "MetricsAggregator",
            Self::FiniteSource => "FiniteSource",
            Self::AsyncFiniteSource => "AsyncFiniteSource",
            Self::InfiniteSource => "InfiniteSource",
            Self::AsyncInfiniteSource => "AsyncInfiniteSource",
            Self::Transform => "Transform",
            Self::Stateful => "Stateful",
            Self::Join => "Join",
            Self::Sink => "Sink",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisionMode {
    SelfSupervised,
    HandlerSupervised,
}

/// The registration event's writer ID identifies this supervisor instance.
/// Names and kinds are supplied by the runtime owner, never guessed by readers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorDescriptor {
    pub name: String,
    pub kind: SupervisorKind,
    pub supervision: SupervisionMode,
}

impl SupervisorDescriptor {
    pub fn validate(&self, writer: &crate::WriterId) -> Result<(), &'static str> {
        if self.name.trim().is_empty() {
            return Err("supervisor name must not be empty");
        }
        let runtime = matches!(
            self.kind,
            SupervisorKind::Pipeline | SupervisorKind::MetricsAggregator
        );
        let expected = if runtime {
            SupervisionMode::SelfSupervised
        } else {
            SupervisionMode::HandlerSupervised
        };
        if self.supervision != expected || runtime != writer.is_system() {
            return Err("supervisor kind, supervision mode and writer identity disagree");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{JournalRecord, SystemEvent, SystemPayload};
    use crate::{JournalWriterId, StageId};
    use serde_json::json;

    #[test]
    fn registration_requires_a_complete_descriptor_matching_its_writer() {
        let descriptor = SupervisorDescriptor {
            name: "orders".into(),
            kind: SupervisorKind::Transform,
            supervision: SupervisionMode::HandlerSupervised,
        };
        let record = JournalRecord::new(
            JournalWriterId::new(),
            SystemEvent::new(
                StageId::new().into(),
                SystemPayload::SupervisorRegistered { descriptor },
            ),
        );
        let encoded = serde_json::to_value(&record).unwrap();
        let decoded: JournalRecord<SystemPayload> =
            serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(decoded.writer_id(), record.writer_id());
        for (field, value) in [
            ("name", json!("")),
            ("kind", json!("pipeline")),
            ("supervision", json!("self_supervised")),
        ] {
            let mut invalid = encoded.clone();
            invalid["payload"]["descriptor"][field] = value;
            assert!(serde_json::from_value::<JournalRecord<SystemPayload>>(invalid).is_err());
        }
        for field in ["name", "kind", "supervision"] {
            let mut missing = encoded.clone();
            missing["payload"]["descriptor"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(serde_json::from_value::<JournalRecord<SystemPayload>>(missing).is_err());
        }
    }
}
