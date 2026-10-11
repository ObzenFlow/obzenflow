// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084l: sender-supplied request identity.
//!
//! Identity is the sender's request ID plus a digest of what the sender
//! submitted. The digest never depends on the application decoder, so a decoder
//! change cannot turn an identical retry into a conflict.

use crate::event::EventId;
use crate::source::{canonicalize, lower_hex};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use std::fmt;

/// A sender's stable request ID: 1 to 255 characters of `A-Z`, `a-z`, `0-9`,
/// `.`, `_`, `:` or `-`, which covers UUIDs and ULIDs.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IngressRequestId(String);

impl IngressRequestId {
    pub const MAX_LEN: usize = 255;

    pub fn parse(value: &str) -> Result<Self, InvalidIngressRequestId> {
        let valid = !value.is_empty()
            && value.len() <= Self::MAX_LEN
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            });
        if valid {
            Ok(Self(value.to_string()))
        } else {
            Err(InvalidIngressRequestId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for IngressRequestId {
    type Error = InvalidIngressRequestId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<IngressRequestId> for String {
    fn from(id: IngressRequestId) -> Self {
        id.0
    }
}

impl fmt::Debug for IngressRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("IngressRequestId").field(&self.0).finish()
    }
}

impl fmt::Display for IngressRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A request ID outside the accepted alphabet or length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("request ID must be 1 to 255 characters of A-Z, a-z, 0-9, '.', '_', ':' or '-'")]
pub struct InvalidIngressRequestId;

/// SHA-256 over domain-tagged, sorted-key canonical JSON of submitted content.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct IngressContentDigest([u8; 32]);

const SUBMITTED_DOMAIN: &[u8] = b"obzenflow.ingress.submitted.v1\0";
const TYPED_DOMAIN: &[u8] = b"obzenflow.ingress.typed.v1\0";

impl IngressContentDigest {
    /// A protocol submission: its event type and `data`. Metadata is excluded.
    pub fn of_submission(event_type: &str, data: &Value) -> Self {
        let content = Value::Object(Map::from_iter([
            (
                "event_type".to_string(),
                Value::String(event_type.to_string()),
            ),
            ("data".to_string(), data.clone()),
        ]));
        Self::tagged(SUBMITTED_DOMAIN, content)
    }

    /// A typed in-process value in its serde form.
    pub fn of_typed(value: Value) -> Self {
        Self::tagged(TYPED_DOMAIN, value)
    }

    fn tagged(domain: &[u8], content: Value) -> Self {
        let mut bytes = domain.to_vec();
        serde_json::to_writer(&mut bytes, &canonicalize(content))
            .expect("JSON values always serialize");
        let mut out = [0_u8; 32];
        out.copy_from_slice(digest(&SHA256, &bytes).as_ref());
        Self(out)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for IngressContentDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IngressContentDigest({})", lower_hex(&self.0))
    }
}

impl Serialize for IngressContentDigest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&lower_hex(&self.0))
    }
}

impl<'de> Deserialize<'de> for IngressContentDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let invalid = || serde::de::Error::custom("expected 64 lowercase hex characters");
        if text.len() != 64 {
            return Err(invalid());
        }
        let mut out = [0_u8; 32];
        for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
            let nibble = |byte: u8| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                _ => None,
            };
            let (Some(high), Some(low)) = (nibble(pair[0]), nibble(pair[1])) else {
                return Err(invalid());
            };
            out[index] = (high << 4) | low;
        }
        Ok(Self(out))
    }
}

/// Request ID plus content digest, committed on the source row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressRequestIdentity {
    pub id: IngressRequestId,
    pub digest: IngressContentDigest,
}

/// The single writer's answer for one submitted member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressReceipt {
    /// This member's row committed.
    Committed { event_id: EventId },
    /// An equal request already committed, or this one joined its staged original.
    Duplicate { event_id: EventId },
    /// The request ID was already used with different content.
    Conflict,
}

/// One recognized member of a duplicate attempt and the row it already has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressOriginal {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<usize>,
    pub event_id: EventId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_ids_accept_the_documented_alphabet_only() {
        assert!(IngressRequestId::parse("tx-0001").is_ok());
        assert!(IngressRequestId::parse("01JZ7Q3M2T8V5K9W4X6Y1N0P3R").is_ok());
        assert!(IngressRequestId::parse("a.b_c:d-e").is_ok());
        assert!(IngressRequestId::parse(&"x".repeat(255)).is_ok());
        assert_eq!(
            IngressRequestId::parse(&"x".repeat(256)),
            Err(InvalidIngressRequestId)
        );
        assert_eq!(IngressRequestId::parse(""), Err(InvalidIngressRequestId));
        assert_eq!(
            IngressRequestId::parse("tx 1"),
            Err(InvalidIngressRequestId)
        );
        assert_eq!(
            IngressRequestId::parse("\"tx\""),
            Err(InvalidIngressRequestId)
        );
        assert_eq!(
            IngressRequestId::parse("tx/1"),
            Err(InvalidIngressRequestId)
        );
    }

    #[test]
    fn request_ids_round_trip_and_refuse_invalid_records() {
        let id = IngressRequestId::parse("tx-0001").unwrap();
        let encoded = serde_json::to_value(&id).unwrap();
        assert_eq!(encoded, json!("tx-0001"));
        assert_eq!(
            serde_json::from_value::<IngressRequestId>(encoded).unwrap(),
            id
        );
        assert!(serde_json::from_value::<IngressRequestId>(json!("bad id")).is_err());
    }

    #[test]
    fn submission_digest_ignores_key_order_and_detects_content_changes() {
        let first = IngressContentDigest::of_submission(
            "bank.ledger_entry",
            &json!({"account_id": "acct-1", "amount_cents": 250}),
        );
        let reordered = IngressContentDigest::of_submission(
            "bank.ledger_entry",
            &json!({"amount_cents": 250, "account_id": "acct-1"}),
        );
        let changed = IngressContentDigest::of_submission(
            "bank.ledger_entry",
            &json!({"account_id": "acct-1", "amount_cents": 999}),
        );
        let other_type = IngressContentDigest::of_submission(
            "bank.other",
            &json!({"account_id": "acct-1", "amount_cents": 250}),
        );
        assert_eq!(first, reordered);
        assert_ne!(first, changed);
        assert_ne!(first, other_type);
    }

    #[test]
    fn submitted_and_typed_forms_are_domain_separated() {
        let data = json!({"event_type": "x", "data": {"n": 1}});
        assert_ne!(
            IngressContentDigest::of_submission("x", &json!({"n": 1})),
            IngressContentDigest::of_typed(data)
        );
    }

    #[test]
    fn digests_serialize_as_lowercase_hex() {
        let digest = IngressContentDigest::of_typed(json!({"n": 1}));
        let encoded = serde_json::to_value(digest).unwrap();
        let text = encoded.as_str().unwrap();
        assert_eq!(text.len(), 64);
        assert!(text
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
        assert_eq!(
            serde_json::from_value::<IngressContentDigest>(encoded).unwrap(),
            digest
        );
        assert!(serde_json::from_value::<IngressContentDigest>(json!("AB")).is_err());
    }
}
