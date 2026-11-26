// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! This module holds the portable data of sources, such as the queries that a source
//! asks and the events that a sender pushes.

mod input;
mod query;

pub use input::EventReceived;
pub use query::{QueryBookmark, QueryError};

use serde_json::{Map, Value};

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceContinuation {
    RecordedRead,
    Refuse,
    Reattach {
        guarantee: crate::event::payloads::execution_payload::ReattachGuarantee,
    },
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeRefusal {
    UnfinishedInput,
}

impl SourceContinuation {
    pub fn resume_refusal(
        self,
        recorded_eof: Option<crate::event::payloads::flow_control_payload::EofKind>,
    ) -> Option<ResumeRefusal> {
        use crate::event::payloads::flow_control_payload::EofKind;
        match self {
            Self::Refuse if recorded_eof != Some(EofKind::Natural) => {
                Some(ResumeRefusal::UnfinishedInput)
            }
            Self::Refuse | Self::RecordedRead | Self::Reattach { .. } => None,
        }
    }
}

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

    #[test]
    fn refuse_admits_resume_only_after_natural_completion() {
        let refuse = SourceContinuation::Refuse;
        let unfinished = Some(ResumeRefusal::UnfinishedInput);
        assert_eq!(refuse.resume_refusal(Some(EofKind::Natural)), None);
        assert_eq!(
            refuse.resume_refusal(Some(EofKind::Poison)),
            unfinished,
            "a Jonestown poison is not natural completion"
        );
        assert_eq!(
            refuse.resume_refusal(Some(EofKind::Truncated)),
            unfinished,
            "an operator stop leaves the input unfinished"
        );
        assert_eq!(
            refuse.resume_refusal(None),
            unfinished,
            "a torn source committed no completion"
        );
    }

    #[test]
    fn reattach_admits_resume_whatever_its_recorded_completion() {
        for guarantee in [
            ReattachGuarantee::SenderRedelivers,
            ReattachGuarantee::AtMostOnce,
        ] {
            for recorded_eof in [
                None,
                Some(EofKind::Natural),
                Some(EofKind::Poison),
                Some(EofKind::Truncated),
            ] {
                let reattach = SourceContinuation::Reattach { guarantee };
                assert_eq!(reattach.resume_refusal(recorded_eof), None);
            }
        }
    }

    #[test]
    fn recorded_read_any_completion_admits_resume() {
        for recorded_eof in [
            None,
            Some(EofKind::Natural),
            Some(EofKind::Poison),
            Some(EofKind::Truncated),
        ] {
            assert_eq!(
                SourceContinuation::RecordedRead.resume_refusal(recorded_eof),
                None
            );
        }
    }
}
