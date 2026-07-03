// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Env-backed secret references (FLOWIP-010 §5, §13).
//!
//! Config carries the reference NAME (visible, non-secret); the resolved
//! secret value exists only at point of use as a [`SecretString`], which
//! redacts in Debug/Display and has no Serialize impl.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A reference to a secret held in an environment variable. The name is the
/// non-secret "reference shape" and stays visible in config and evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretRef {
    env: String,
}

impl SecretRef {
    pub fn new(env: impl Into<String>) -> Self {
        Self { env: env.into() }
    }

    pub fn env_name(&self) -> &str {
        &self.env
    }

    /// Resolve the referenced variable to secret material.
    pub fn resolve(&self) -> Result<SecretString, SecretResolveError> {
        match std::env::var(&self.env) {
            Ok(value) => Ok(SecretString(value)),
            Err(std::env::VarError::NotPresent) => Err(SecretResolveError::Missing {
                env: self.env.clone(),
            }),
            Err(std::env::VarError::NotUnicode(_)) => Err(SecretResolveError::NotUnicode {
                env: self.env.clone(),
            }),
        }
    }
}

/// Resolved secret material. Redacts in Debug/Display; never serialized.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(***redacted***)")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***redacted***")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretResolveError {
    Missing { env: String },
    NotUnicode { env: String },
}

impl fmt::Display for SecretResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { env } => {
                write!(f, "secret reference {env} is not set in the environment")
            }
            Self::NotUnicode { env } => {
                write!(f, "secret reference {env} holds non-unicode data")
            }
        }
    }
}

impl std::error::Error for SecretResolveError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_string_redacts_in_debug_and_display() {
        let secret = SecretString("hunter2".to_string());
        assert_eq!(format!("{secret:?}"), "SecretString(***redacted***)");
        assert_eq!(secret.to_string(), "***redacted***");
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn secret_ref_serializes_as_the_visible_reference_name() {
        let reference = SecretRef::new("OPENAI_API_KEY");
        assert_eq!(
            serde_json::to_string(&reference).unwrap(),
            "\"OPENAI_API_KEY\""
        );
    }
}
