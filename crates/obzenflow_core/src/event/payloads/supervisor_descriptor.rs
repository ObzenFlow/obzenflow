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
    pub const fn is_runtime(self) -> bool {
        matches!(self, Self::Pipeline | Self::MetricsAggregator)
    }

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
    pub fn event_prefix(&self) -> String {
        supervisor_event_prefix(&self.name, self.supervision)
    }

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

/// Canonical supervisor namespace. Encode UTF-8 bytes outside the ordinary name
/// alphabet, including both the separator and escape character, without loss.
pub fn supervisor_event_prefix(name: &str, mode: SupervisionMode) -> String {
    use std::fmt::Write;
    let family = if mode == SupervisionMode::SelfSupervised {
        "runtime"
    } else {
        "stage"
    };
    let mut prefix = format!("supervisor.{family}.");
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') {
            prefix.push(char::from(byte));
        } else {
            write!(&mut prefix, "%{byte:02X}").expect("writing to a String");
        }
    }
    prefix
}

pub fn supervisor_event_type(name: &str, mode: SupervisionMode, occurrence: &str) -> String {
    format!("{}.{occurrence}", supervisor_event_prefix(name, mode))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{JournalRecord, SystemEvent, SystemPayload};
    use crate::{JournalWriterId, StageId};
    use serde_json::json;

    #[test]
    fn canonical_prefix_preserves_names_without_separator_collisions() {
        for (name, encoded) in [
            ("validate_order", "validate_order"),
            ("orders.v2", "orders%2Ev2"),
            ("orders%2Ev2", "orders%252Ev2"),
            ("café/entrée", "caf%C3%A9%2Fentr%C3%A9e"),
            ("order service", "order%20service"),
        ] {
            assert_eq!(
                supervisor_event_prefix(name, SupervisionMode::HandlerSupervised),
                format!("supervisor.stage.{encoded}")
            );
            assert_eq!(
                supervisor_event_prefix(name, SupervisionMode::SelfSupervised),
                format!("supervisor.runtime.{encoded}")
            );
        }
    }

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
