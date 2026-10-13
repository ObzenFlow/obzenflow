// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Env-backed secret references.
//!
//! Config carries the reference NAME (visible, non-secret); the resolved
//! secret value exists only at point of use as a [`SecretString`], which
//! redacts in Debug/Display and has no Serialize impl. A reference admits
//! only an environment-variable name, so the visible name is never secret
//! material.

use serde::de::{self, Deserializer, Unexpected, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A reference to a secret held in an environment variable. The name is the
/// non-secret "reference shape" and stays visible in config and evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SecretRef {
    env: String,
}

impl SecretRef {
    /// Refuses text that is not an environment-variable name, without
    /// keeping it.
    pub fn new(env: impl Into<String>) -> Result<Self, InvalidSecretRef> {
        let env = env.into();
        if is_env_name(&env) {
            Ok(Self { env })
        } else {
            Err(InvalidSecretRef)
        }
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

impl<'de> Deserialize<'de> for SecretRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EnvName;

        impl Visitor<'_> for EnvName {
            type Value = SecretRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an environment variable name")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<SecretRef, E> {
                SecretRef::new(value)
                    .map_err(|_| E::invalid_value(Unexpected::Other("text"), &self))
            }
        }

        deserializer.deserialize_str(EnvName)
    }
}

/// True when `text` holds only uppercase letters, digits and underscores and
/// does not start with a digit.
pub fn is_env_name(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_uppercase() || first == '_')
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Text refused as a secret reference. It keeps nothing of the text, so a
/// pasted secret reaches no error, log or `Debug` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSecretRef;

impl fmt::Display for InvalidSecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "expected an environment variable name: uppercase letters, digits and underscores, \
             not starting with a digit",
        )
    }
}

impl std::error::Error for InvalidSecretRef {}

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

    const SENTINEL: &str = "sk_live_51HSENTINEL";

    #[test]
    fn secret_string_redacts_in_debug_and_display() {
        let secret = SecretString("hunter2".to_string());
        assert_eq!(format!("{secret:?}"), "SecretString(***redacted***)");
        assert_eq!(secret.to_string(), "***redacted***");
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn secret_ref_serializes_as_the_visible_reference_name() {
        let reference = SecretRef::new("OPENAI_API_KEY").unwrap();
        assert_eq!(
            serde_json::to_string(&reference).unwrap(),
            "\"OPENAI_API_KEY\""
        );
    }

    #[test]
    fn only_uppercase_environment_variable_names_are_admitted() {
        for name in ["OPENAI_API_KEY", "_PRIVATE", "KEY2", "A"] {
            assert_eq!(SecretRef::new(name).unwrap().env_name(), name);
        }
        for text in [
            "",
            "2FA_KEY",
            "openai_api_key",
            "Mixed_Case",
            "HAS SPACE",
            "HAS-DASH",
            "ÉTÉ",
            SENTINEL,
            "ghp_0123456789abcdef",
        ] {
            assert_eq!(SecretRef::new(text), Err(InvalidSecretRef), "{text:?}");
        }
    }

    #[test]
    fn a_refused_reference_echoes_nothing_through_serde_json() {
        let error = SecretRef::new(SENTINEL).unwrap_err();
        assert!(!format!("{error} {error:?}").contains(SENTINEL));

        let json = serde_json::from_str::<SecretRef>(&format!("\"{SENTINEL}\"")).unwrap_err();
        let rendered = format!("{json} {json:?}");
        assert!(!rendered.contains(SENTINEL), "{rendered}");
        assert!(
            rendered.contains("expected an environment variable name"),
            "{rendered}"
        );
    }

    #[test]
    fn an_admitted_reference_deserializes_from_serde_json() {
        let reference: SecretRef = serde_json::from_str("\"PARTNER_API_KEY\"").unwrap();
        assert_eq!(reference.env_name(), "PARTNER_API_KEY");
    }
}
