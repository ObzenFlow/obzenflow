// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::payloads::effect_payload::{EffectInputPosition, EffectOrdinal, EffectType};
use crate::event::schema::TypedPayload;
use crate::{EventId, EventType, StageId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageFatalSeverity {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageFatalCode {
    Protocol,
    Replay,
    Configuration,
    Journal,
    Coordination,
    Resource,
    Termination,
}

/// Closed metric and diagnostic authority for stage-fatal failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageFatalReason {
    ProtocolInputIntegrity,
    ReplayDivergence,
    /// A replay reached a cursor its recording holds no outcome for; only a
    /// resume finishes that work (FLOWIP-134i B13).
    RecordingEnded,
    /// A rebuilt effect's identity differs from its recording.
    DescriptorMismatch,
    /// The rebuild performed an effect its recording lacks.
    MissingRecordedEffect,
    /// The rebuild skipped an effect its recording holds.
    UnusedEffectHistory,
    /// A resume emitted a fact its recording lacks before the recording ends.
    MissingRecordedFact,
    /// A resume skipped a fact its recording holds.
    UnusedFactHistory,
    ConfigurationInvariant,
    EffectTargetAssertionMismatch,
    EffectPortRegistrationMissing,
    EffectPortResolutionFailed,
    EffectPortBindingMismatch,
    EffectPortTargetInvariantViolation,
    JournalFailure,
    CoordinationFailure,
    ResourceExhaustion,
    IncompleteTermination,
}

impl StageFatalReason {
    /// Whether a resume refuses with this reason because its build or
    /// configuration departs from its archive.
    pub fn departs_from_archive(self) -> bool {
        matches!(
            self,
            Self::DescriptorMismatch
                | Self::MissingRecordedEffect
                | Self::UnusedEffectHistory
                | Self::MissingRecordedFact
                | Self::UnusedFactHistory
        )
    }
}

/// The two actions a rebuild's check compared when it refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Departure {
    /// The action the rebuild offered, absent when its input ended without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offered: Option<DepartureAction>,
    /// The archived action it was compared with.
    pub expected: DepartureAction,
}

/// An action a departure names, by its kind and the identity of that kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DepartureAction {
    /// An effect call, at its input and ordinal.
    Effect {
        effect_type: EffectType,
        input_position: EffectInputPosition,
        effect_ordinal: EffectOrdinal,
    },
    /// An emitted fact.
    Fact {
        event_type: EventType,
        event_id: EventId,
    },
    /// An input settled without an effect, a capture or a fact.
    Settlement { input_position: EffectInputPosition },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageFatalRecorded {
    pub severity: StageFatalSeverity,
    pub stage_id: StageId,
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_position: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_cause_event_id: Option<EventId>,
    pub code: StageFatalCode,
    pub reason: StageFatalReason,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub departure: Option<Departure>,
}

impl TypedPayload for StageFatalRecorded {
    const EVENT_KIND: crate::event::payloads::chain_payload::EventKind =
        crate::event::payloads::chain_payload::EventKind::Execution;
    fn into_chain_payload(self) -> Result<crate::event::ChainPayload, serde_json::Error> {
        Ok(crate::event::ChainPayload::Execution(
            super::execution_payload::ExecutionPayload::StageFatalRecorded(self),
        ))
    }
    fn accepts_payload(payload: &crate::event::ChainPayload) -> bool {
        matches!(
            payload,
            crate::event::ChainPayload::Execution(
                super::execution_payload::ExecutionPayload::StageFatalRecorded(_)
            )
        )
    }

    const EVENT_TYPE: &'static str = "obzenflow.stage_fatal_recorded";
    const SCHEMA_VERSION: u32 = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(departure: Option<Departure>) -> StageFatalRecorded {
        StageFatalRecorded {
            severity: StageFatalSeverity::Primary,
            stage_id: StageId::new(),
            stage_key: "allocate_stock".to_string(),
            causal_event_id: None,
            input_position: Some(5),
            primary_cause_event_id: None,
            code: StageFatalCode::Replay,
            reason: StageFatalReason::MissingRecordedFact,
            detail: "stage 'allocate_stock' emitted CancelIgnored at input 5, but its recording \
                     settled that input without an action"
                .to_string(),
            departure,
        }
    }

    #[test]
    fn a_departure_records_both_actions_under_their_approved_spellings() {
        let event_id = EventId::new();
        let departure = Departure {
            offered: Some(DepartureAction::Fact {
                event_type: EventType::from("allocation.cancel_ignored"),
                event_id,
            }),
            expected: DepartureAction::Settlement {
                input_position: EffectInputPosition::new(5),
            },
        };
        let value = serde_json::to_value(row(Some(departure.clone()))).unwrap();
        assert_eq!(
            value["departure"],
            json!({
                "offered": {
                    "kind": "fact",
                    "event_type": "allocation.cancel_ignored",
                    "event_id": serde_json::to_value(event_id).unwrap(),
                },
                "expected": { "kind": "settlement", "input_position": 5 },
            })
        );
        let read: StageFatalRecorded = serde_json::from_value(value).unwrap();
        assert_eq!(read.departure, Some(departure));
    }

    #[test]
    fn an_effect_names_its_type_input_and_ordinal_and_an_ended_input_offers_nothing() {
        let departure = Departure {
            offered: None,
            expected: DepartureAction::Effect {
                effect_type: EffectType::from("allocation.reserve_stock"),
                input_position: EffectInputPosition::new(7),
                effect_ordinal: EffectOrdinal::new(1),
            },
        };
        assert_eq!(
            serde_json::to_value(&departure).unwrap(),
            json!({
                "expected": {
                    "kind": "effect",
                    "effect_type": "allocation.reserve_stock",
                    "input_position": 7,
                    "effect_ordinal": 1,
                },
            })
        );
    }

    #[test]
    fn a_row_without_a_departure_writes_none_and_reads_back_without_one() {
        let value = serde_json::to_value(row(None)).unwrap();
        assert!(value.get("departure").is_none());
        let read: StageFatalRecorded = serde_json::from_value(value).unwrap();
        assert_eq!(read.departure, None);
    }
}
