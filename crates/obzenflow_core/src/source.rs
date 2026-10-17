// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source positions, progress and input identity (FLOWIP-134i B1, B9, B11).
//!
//! A positioned reader reports the position immediately after each record. The
//! runtime encodes it into that record's protected `source_progress`
//! provenance, so a fact and its position commit in one append. Input identity
//! lives once, in the source's input pin. Neither is ever displayed: errors
//! name what failed, never a position or an identity value.

use crate::event::payloads::execution_payload::{
    ExecutionPayload, SourceContinuationRefusal, SourceInputPinnedFact,
};
use crate::event::ChainPayload;
use ring::digest::{digest, SHA256};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::num::NonZeroU32;

/// Upper bound on an encoded position's canonical JSON size.
pub const MAX_ENCODED_POSITION_BYTES: usize = 512;

/// A connector's position in its input.
pub trait SourcePosition: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Exact representation version. The pin records it and resume refuses any
    /// other. Zero is a compile error:
    ///
    /// ```compile_fail
    /// use obzenflow_core::source::{EncodedSourcePosition, SourcePosition};
    /// #[derive(Clone, serde::Serialize, serde::Deserialize)]
    /// struct Offset(u64);
    /// impl SourcePosition for Offset {
    ///     const SCHEMA_VERSION: u32 = 0;
    /// }
    /// let _ = EncodedSourcePosition::encode(&Offset(0));
    /// ```
    const SCHEMA_VERSION: u32 = 1;
}

/// `P`'s representation version, as a constant for declarations that carry
/// it (FLOWIP-134i B22).
pub const fn position_version<P: SourcePosition>() -> NonZeroU32 {
    match NonZeroU32::new(P::SCHEMA_VERSION) {
        Some(version) => version,
        None => panic!("SourcePosition::SCHEMA_VERSION must be positive"),
    }
}

fn encoding_version<P: SourcePosition>() -> NonZeroU32 {
    const { position_version::<P>() }
}

/// Why a position could not be encoded or decoded. It never carries the
/// position itself.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourcePositionError {
    #[error("the source position could not be encoded")]
    Unencodable,
    #[error(
        "the source position encodes to {size} bytes, above the {MAX_ENCODED_POSITION_BYTES}-byte bound"
    )]
    Oversize { size: usize },
    #[error("the recorded source position has version {recorded}, but the connector reads version {expected}")]
    VersionMismatch {
        recorded: NonZeroU32,
        expected: NonZeroU32,
    },
    #[error("the recorded source position could not be decoded")]
    Undecodable,
}

/// A position encoded for storage with its representation version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncodedSourcePosition {
    schema_version: NonZeroU32,
    value: Value,
}

impl EncodedSourcePosition {
    pub fn encode<P: SourcePosition>(position: &P) -> Result<Self, SourcePositionError> {
        let value = serde_json::to_value(position).map_err(|_| SourcePositionError::Unencodable)?;
        let encoded = Self {
            schema_version: encoding_version::<P>(),
            value,
        };
        encoded.check_size()?;
        Ok(encoded)
    }

    pub fn decode<P: SourcePosition>(&self) -> Result<P, SourcePositionError> {
        let expected = encoding_version::<P>();
        if self.schema_version != expected {
            return Err(SourcePositionError::VersionMismatch {
                recorded: self.schema_version,
                expected,
            });
        }
        self.check_size()?;
        serde_json::from_value(self.value.clone()).map_err(|_| SourcePositionError::Undecodable)
    }

    pub fn schema_version(&self) -> NonZeroU32 {
        self.schema_version
    }

    fn check_size(&self) -> Result<(), SourcePositionError> {
        let size = serde_json::to_vec(&self.value)
            .map_err(|_| SourcePositionError::Unencodable)?
            .len();
        if size > MAX_ENCODED_POSITION_BYTES {
            return Err(SourcePositionError::Oversize { size });
        }
        Ok(())
    }
}

/// The position after one source record, in that record's protected
/// provenance (FLOWIP-134i B11). Derived facts never inherit it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceProgress {
    pub after: EncodedSourcePosition,
}

/// Where a positioned source continues on resume (FLOWIP-134i B1, B15).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceHandoff {
    /// Validate the pin, then continue strictly after `after`, or at the
    /// pinned beginning when nothing was admitted after it.
    Seek {
        pin: SourceInputPinnedFact,
        after: Option<EncodedSourcePosition>,
    },
    /// Never pinned and nothing admitted: pin and read as a fresh run (B15).
    Start,
}

/// Why a recorded frontier cannot be continued (FLOWIP-134i B22).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFrontierRefusal {
    /// An admitted record lacks its position or a preceding pin (B11).
    MissingEvidence,
    /// The recorded positions use another version of the connector's format.
    PositionVersion {
        recorded: NonZeroU32,
        expected: NonZeroU32,
    },
}

impl From<SourceFrontierRefusal> for SourceContinuationRefusal {
    fn from(refusal: SourceFrontierRefusal) -> Self {
        match refusal {
            SourceFrontierRefusal::MissingEvidence => Self::MissingEvidence,
            SourceFrontierRefusal::PositionVersion { .. } => Self::PositionUnreadable,
        }
    }
}

/// A source journal's committed frontier, folded in journal order
/// (FLOWIP-134i B11): the latest input pin and the position after the last
/// admitted record bound to it. Archive admission and the replay handoff both
/// apply [`Self::handoff`] to it (B22).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecordedSourceFrontier {
    pin: Option<SourceInputPinnedFact>,
    after: Option<EncodedSourcePosition>,
    admitted: u64,
    unpositioned: bool,
}

impl RecordedSourceFrontier {
    /// Fold one record a replay re-admits or preserves: its payload and the
    /// position its provenance carries.
    pub fn observe(&mut self, payload: &ChainPayload, progress: Option<&SourceProgress>) {
        match payload {
            ChainPayload::Execution(ExecutionPayload::SourceInputPinned(pin)) => {
                self.pin = Some(pin.clone());
                self.after = None;
            }
            ChainPayload::Fact(_) => {
                self.admitted = self.admitted.saturating_add(1);
                match (progress, &self.pin) {
                    (Some(progress), Some(_)) => self.after = Some(progress.after.clone()),
                    _ => self.unpositioned = true,
                }
            }
            _ => {}
        }
    }

    /// Where a connector positioned in version `expected` continues: a
    /// positioned record with no preceding pin, or an admitted record without
    /// a position, is missing evidence.
    pub fn handoff(&self, expected: NonZeroU32) -> Result<SourceHandoff, SourceFrontierRefusal> {
        if self.unpositioned {
            return Err(SourceFrontierRefusal::MissingEvidence);
        }
        let Some(pin) = &self.pin else {
            return if self.admitted == 0 {
                Ok(SourceHandoff::Start)
            } else {
                Err(SourceFrontierRefusal::MissingEvidence)
            };
        };
        let recorded = self
            .after
            .as_ref()
            .unwrap_or(&pin.beginning)
            .schema_version();
        if recorded != expected {
            return Err(SourceFrontierRefusal::PositionVersion { recorded, expected });
        }
        Ok(SourceHandoff::Seek {
            pin: pin.clone(),
            after: self.after.clone(),
        })
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceContinuation {
    Refuse {
        by: RefusedBy,
    },
    Seek {
        position_version: NonZeroU32,
    },
    Reattach {
        guarantee: crate::event::payloads::execution_payload::ReattachGuarantee,
    },
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusedBy {
    Connector,
    Handler,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeRefusal {
    UnfinishedInput {
        by: RefusedBy,
    },
    RecordedWithoutPositions,
    PositionVersion {
        recorded: NonZeroU32,
        expected: NonZeroU32,
    },
}

impl SourceContinuation {
    pub fn resume_refusal(
        self,
        recorded_eof: Option<crate::event::payloads::flow_control_payload::EofKind>,
        frontier: &RecordedSourceFrontier,
    ) -> Option<ResumeRefusal> {
        use crate::event::payloads::flow_control_payload::EofKind;
        if recorded_eof == Some(EofKind::Natural) {
            return None;
        }
        match self {
            Self::Refuse { by } => Some(ResumeRefusal::UnfinishedInput { by }),
            Self::Seek { position_version } => {
                frontier
                    .handoff(position_version)
                    .err()
                    .map(|refusal| match refusal {
                        SourceFrontierRefusal::MissingEvidence => {
                            ResumeRefusal::RecordedWithoutPositions
                        }
                        SourceFrontierRefusal::PositionVersion { recorded, expected } => {
                            ResumeRefusal::PositionVersion { recorded, expected }
                        }
                    })
            }
            Self::Reattach { .. } => None,
        }
    }
}

/// Why an input identity could not be computed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceIdentityError {
    #[error("the source input identity parts could not be encoded")]
    Unencodable,
    #[error("the source input identity parts must be a struct or map of named parts")]
    NotNamedParts,
}

/// What decides a source's record boundaries and order (FLOWIP-134i B9): one
/// digest per named part, so a refusal can name the part that changed without
/// revealing it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceInputIdentity {
    kind: String,
    parts: BTreeMap<String, String>,
}

impl SourceInputIdentity {
    /// One identity over `parts`, each of whose top-level fields names a part.
    pub fn new<T: Serialize>(kind: &str, parts: &T) -> Result<Self, SourceIdentityError> {
        let Value::Object(fields) =
            serde_json::to_value(parts).map_err(|_| SourceIdentityError::Unencodable)?
        else {
            return Err(SourceIdentityError::NotNamedParts);
        };
        let parts = fields
            .into_iter()
            .map(|(name, value)| Ok((name, canonical_digest(value)?)))
            .collect::<Result<_, SourceIdentityError>>()?;
        Ok(Self {
            kind: kind.to_string(),
            parts,
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The first part, by name, that differs from `recorded`; `kind` when the
    /// connector kind itself differs.
    pub fn changed_part(&self, recorded: &Self) -> Option<String> {
        if self.kind != recorded.kind {
            return Some("kind".to_string());
        }
        self.parts
            .keys()
            .chain(recorded.parts.keys())
            .filter(|name| self.parts.get(*name) != recorded.parts.get(*name))
            .min()
            .cloned()
    }

    /// One digest over the whole identity, for evidence that names it.
    pub fn digest(&self) -> String {
        let mut bytes = Vec::new();
        let mut field = |text: &str| {
            bytes.extend_from_slice(&(text.len() as u64).to_le_bytes());
            bytes.extend_from_slice(text.as_bytes());
        };
        field(&self.kind);
        for (name, part) in &self.parts {
            field(name);
            field(part);
        }
        hex_sha256(&bytes)
    }
}

fn canonical_digest(value: Value) -> Result<String, SourceIdentityError> {
    let bytes =
        serde_json::to_vec(&canonicalize(value)).map_err(|_| SourceIdentityError::Unencodable)?;
    Ok(hex_sha256(&bytes))
}

/// Sorted-key JSON, arrays in order: the canonical form hashed by digests.
pub(crate) fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize(value)))
                    .collect::<Map<_, _>>(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        scalar => scalar,
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    lower_hex(digest(&SHA256, bytes).as_ref())
}

pub(crate) fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        hex.push(char::from(HEX[usize::from(byte >> 4)]));
        hex.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::payloads::execution_payload::ReattachGuarantee;
    use crate::event::payloads::flow_control_payload::EofKind;

    fn refusal(
        continuation: SourceContinuation,
        recorded_eof: Option<EofKind>,
    ) -> Option<ResumeRefusal> {
        continuation.resume_refusal(recorded_eof, &RecordedSourceFrontier::default())
    }

    #[test]
    fn refuse_admits_resume_only_after_natural_completion() {
        for by in [RefusedBy::Connector, RefusedBy::Handler] {
            let refuse = SourceContinuation::Refuse { by };
            let unfinished = Some(ResumeRefusal::UnfinishedInput { by });
            assert_eq!(refusal(refuse, Some(EofKind::Natural)), None);
            assert_eq!(
                refusal(refuse, Some(EofKind::Poison)),
                unfinished,
                "a Jonestown poison is not natural completion"
            );
            assert_eq!(
                refusal(refuse, Some(EofKind::Truncated)),
                unfinished,
                "an operator stop leaves the input unfinished"
            );
            assert_eq!(
                refusal(refuse, None),
                unfinished,
                "a torn source committed no completion"
            );
        }
    }

    #[test]
    fn reattach_admits_resume_whatever_its_recorded_completion() {
        for guarantee in [
            ReattachGuarantee::SenderRedelivers,
            ReattachGuarantee::LatestState,
            ReattachGuarantee::AtMostOnce,
        ] {
            for recorded_eof in [
                None,
                Some(EofKind::Natural),
                Some(EofKind::Poison),
                Some(EofKind::Truncated),
            ] {
                let reattach = SourceContinuation::Reattach { guarantee };
                assert_eq!(refusal(reattach, recorded_eof), None);
            }
        }
    }

    #[test]
    fn seek_admits_completion_and_a_never_pinned_start() {
        let seek = SourceContinuation::Seek {
            position_version: NonZeroU32::MIN,
        };
        assert_eq!(refusal(seek, Some(EofKind::Natural)), None);
        assert_eq!(refusal(seek, Some(EofKind::Truncated)), None);
        assert_eq!(refusal(seek, None), None);
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Offset {
        record: u64,
    }

    impl SourcePosition for Offset {}

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Wide(String);

    impl SourcePosition for Wide {
        const SCHEMA_VERSION: u32 = 2;
    }

    #[test]
    fn positions_round_trip_with_their_version() {
        let encoded = EncodedSourcePosition::encode(&Offset { record: 300 }).unwrap();
        assert_eq!(encoded.schema_version(), NonZeroU32::MIN);
        assert_eq!(encoded.decode::<Offset>().unwrap(), Offset { record: 300 });
        let restored: EncodedSourcePosition =
            serde_json::from_value(serde_json::to_value(&encoded).unwrap()).unwrap();
        assert_eq!(restored, encoded);
    }

    #[test]
    fn another_version_or_an_oversize_position_is_refused() {
        let encoded = EncodedSourcePosition::encode(&Wide("x".into())).unwrap();
        assert!(matches!(
            encoded.decode::<Offset>(),
            Err(SourcePositionError::VersionMismatch { .. })
        ));
        assert!(matches!(
            EncodedSourcePosition::encode(&Wide("x".repeat(MAX_ENCODED_POSITION_BYTES))),
            Err(SourcePositionError::Oversize { .. })
        ));
        let error = EncodedSourcePosition::encode(&Wide("secret".repeat(200))).unwrap_err();
        assert!(
            !error.to_string().contains("secret"),
            "diagnostics never carry the position"
        );
    }

    #[derive(Serialize)]
    struct Parts<'a> {
        file: &'a str,
        selection: &'a str,
    }

    #[test]
    fn identity_names_the_changed_part_without_revealing_it() {
        let recorded = SourceInputIdentity::new(
            "csv",
            &Parts {
                file: "inode-1",
                selection: "all",
            },
        )
        .unwrap();
        let same = SourceInputIdentity::new(
            "csv",
            &Parts {
                file: "inode-1",
                selection: "all",
            },
        )
        .unwrap();
        assert_eq!(same.changed_part(&recorded), None);
        assert_eq!(same.digest(), recorded.digest());

        let replaced = SourceInputIdentity::new(
            "csv",
            &Parts {
                file: "inode-2",
                selection: "all",
            },
        )
        .unwrap();
        assert_eq!(replaced.changed_part(&recorded).as_deref(), Some("file"));
        assert!(!serde_json::to_string(&replaced)
            .unwrap()
            .contains("inode-2"));

        let other_kind = SourceInputIdentity::new(
            "yaml",
            &Parts {
                file: "inode-1",
                selection: "all",
            },
        )
        .unwrap();
        assert_eq!(other_kind.changed_part(&recorded).as_deref(), Some("kind"));
        assert_eq!(
            SourceInputIdentity::new("csv", &"not named"),
            Err(SourceIdentityError::NotNamedParts)
        );
    }

    fn pin(beginning: &impl SourcePosition) -> crate::ChainEvent {
        crate::event::ChainEventFactory::execution_event(
            crate::WriterId::from(crate::StageId::new()),
            ExecutionPayload::SourceInputPinned(SourceInputPinnedFact {
                identity: SourceInputIdentity::new("test", &serde_json::json!({ "input": "rows" }))
                    .unwrap(),
                beginning: EncodedSourcePosition::encode(beginning).unwrap(),
            }),
        )
    }

    fn record(after: Option<&Offset>) -> crate::ChainEvent {
        let mut event = crate::event::ChainEventFactory::data_event(
            crate::WriterId::from(crate::StageId::new()),
            "test.row",
            NonZeroU32::MIN,
            serde_json::json!({}),
        );
        event.source_progress = after.map(|after| SourceProgress {
            after: EncodedSourcePosition::encode(after).unwrap(),
        });
        event
    }

    fn frontier(events: &[crate::ChainEvent]) -> RecordedSourceFrontier {
        let mut frontier = RecordedSourceFrontier::default();
        for event in events {
            frontier.observe(&event.payload, event.source_progress.as_ref());
        }
        frontier
    }

    /// FLOWIP-134i B11, B15, B22: one rule decides where a recorded source
    /// continues, for admission and the replay handoff alike.
    #[test]
    fn a_recorded_frontier_hands_off_or_names_why_it_cannot() {
        let one = NonZeroU32::MIN;
        let two = NonZeroU32::new(2).unwrap();
        let start = Offset { record: 0 };
        let third = Offset { record: 3 };

        assert_eq!(frontier(&[]).handoff(one), Ok(SourceHandoff::Start));
        let Ok(SourceHandoff::Seek { after, .. }) = frontier(&[pin(&start)]).handoff(one) else {
            panic!("a pinned empty prefix seeks from its beginning");
        };
        assert_eq!(after, None);
        let Ok(SourceHandoff::Seek { after, .. }) =
            frontier(&[pin(&start), record(Some(&third))]).handoff(one)
        else {
            panic!("a positioned prefix seeks after its last record");
        };
        assert_eq!(after.unwrap().decode::<Offset>().unwrap(), third);

        for (events, case) in [
            (vec![record(None)], "records without a pin"),
            (
                vec![pin(&start), record(None)],
                "a record without its position",
            ),
        ] {
            assert_eq!(
                frontier(&events).handoff(one),
                Err(SourceFrontierRefusal::MissingEvidence),
                "{case}"
            );
        }
        assert_eq!(
            frontier(&[pin(&start), record(Some(&third))]).handoff(two),
            Err(SourceFrontierRefusal::PositionVersion {
                recorded: one,
                expected: two,
            })
        );
        assert_eq!(
            frontier(&[pin(&Wide("x".into()))]).handoff(one),
            Err(SourceFrontierRefusal::PositionVersion {
                recorded: two,
                expected: one,
            }),
            "an empty prefix is checked by its pinned beginning"
        );
    }
}
