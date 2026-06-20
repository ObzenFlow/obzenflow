// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Protocol-neutral hosted-ingress identity key.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;

/// Protocol-neutral hosted-ingress identity (FLOWIP-115d). Identifies one hosted
/// ingress surface independently of any HTTP base path, and is carried in
/// accepted-event provenance, `IngressRefusal` facts, and the projected refusal
/// metric. The optional HTTP wrapper derives it from the normalised base path. It
/// is a transparent string newtype mirroring [`crate::EventType`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IngressKey(pub String);

impl IngressKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for IngressKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for IngressKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl AsRef<str> for IngressKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for IngressKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for IngressKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for IngressKey {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<IngressKey> for &str {
    fn eq(&self, other: &IngressKey) -> bool {
        *self == other.as_str()
    }
}
