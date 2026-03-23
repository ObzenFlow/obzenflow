// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed environment-variable parsing helpers.

use std::any::type_name;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvParseError {
    /// Variable is set but its value cannot be parsed as the target type.
    InvalidValue {
        key: String,
        value: String,
        expected: &'static str,
        detail: String,
    },
    /// Variable is required but not set.
    Missing { key: String },
}

impl Display for EnvParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidValue {
                key,
                value,
                expected,
                detail,
            } => write!(
                f,
                "environment variable {key}={value:?}: expected {expected}, {detail}"
            ),
            Self::Missing { key } => {
                write!(f, "environment variable {key}: required but not set")
            }
        }
    }
}

impl std::error::Error for EnvParseError {}

/// Returns `Ok(Some(value))` if the variable is set and parses successfully,
/// `Ok(None)` if the variable is not set or empty after trimming, and `Err(...)`
/// if the variable is set but cannot be parsed.
pub fn env_var<T: FromStr>(key: &str) -> Result<Option<T>, EnvParseError>
where
    T::Err: Display,
{
    let expected = expected_type_name::<T>();
    let Some(value) = read_env_value(key, expected)? else {
        return Ok(None);
    };

    value
        .parse::<T>()
        .map(Some)
        .map_err(|err| EnvParseError::InvalidValue {
            key: key.to_string(),
            value,
            expected,
            detail: format!("got parse error: {err}"),
        })
}

/// Like [`env_var`], but returns the default when the variable is not set.
pub fn env_var_or<T: FromStr>(key: &str, default: T) -> Result<T, EnvParseError>
where
    T::Err: Display,
{
    Ok(env_var(key)?.unwrap_or(default))
}

/// Like [`env_var`], but returns an error when the variable is not set.
pub fn env_var_required<T: FromStr>(key: &str) -> Result<T, EnvParseError>
where
    T::Err: Display,
{
    env_var(key)?.ok_or_else(|| EnvParseError::Missing {
        key: key.to_string(),
    })
}

/// Parses a boolean environment variable with flexible normalisation.
///
/// Accepts: `1`/`true`/`yes`/`y`/`on` and `0`/`false`/`no`/`n`/`off`.
pub fn env_bool(key: &str) -> Result<Option<bool>, EnvParseError> {
    const EXPECTED: &str = "bool";

    let Some(value) = read_env_value(key, EXPECTED)? else {
        return Ok(None);
    };

    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "n" | "off" => Ok(Some(false)),
        _ => Err(EnvParseError::InvalidValue {
            key: key.to_string(),
            value,
            expected: EXPECTED,
            detail: "got unrecognised boolean value (expected one of 1/true/yes/y/on or 0/false/no/n/off)".to_string(),
        }),
    }
}

/// Like [`env_bool`], but returns the default when the variable is not set.
pub fn env_bool_or(key: &str, default: bool) -> Result<bool, EnvParseError> {
    Ok(env_bool(key)?.unwrap_or(default))
}

fn read_env_value(key: &str, expected: &'static str) -> Result<Option<String>, EnvParseError> {
    let Some(raw) = std::env::var_os(key) else {
        return Ok(None);
    };

    let value = raw
        .into_string()
        .map_err(|raw| EnvParseError::InvalidValue {
            key: key.to_string(),
            value: raw.to_string_lossy().into_owned(),
            expected,
            detail: "value is not valid UTF-8".to_string(),
        })?;

    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    Ok(Some(trimmed.to_string()))
}

fn expected_type_name<T>() -> &'static str {
    match type_name::<T>() {
        "alloc::string::String" | "std::string::String" => "String",
        name => name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{env_lock, EnvGuard};

    #[test]
    fn env_var_returns_none_when_missing() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_MISSING"]);
        guard.remove("TEST_ENV_MISSING");

        assert_eq!(env_var::<usize>("TEST_ENV_MISSING").unwrap(), None);
    }

    #[test]
    fn env_var_parses_trimmed_values() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_TRIMMED"]);
        guard.set("TEST_ENV_TRIMMED", " 30 ");

        assert_eq!(env_var::<usize>("TEST_ENV_TRIMMED").unwrap(), Some(30));
    }

    #[test]
    fn env_var_returns_none_for_empty_after_trim() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_EMPTY"]);
        guard.set("TEST_ENV_EMPTY", "   ");

        assert_eq!(env_var::<String>("TEST_ENV_EMPTY").unwrap(), None);
    }

    #[test]
    fn env_var_required_treats_empty_after_trim_as_missing() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_REQUIRED_EMPTY"]);
        guard.set("TEST_ENV_REQUIRED_EMPTY", "   ");

        assert_eq!(
            env_var_required::<String>("TEST_ENV_REQUIRED_EMPTY").unwrap_err(),
            EnvParseError::Missing {
                key: "TEST_ENV_REQUIRED_EMPTY".to_string(),
            }
        );
    }

    #[test]
    fn env_var_reports_invalid_values() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_INVALID"]);
        guard.set("TEST_ENV_INVALID", "abc");

        let err = env_var::<usize>("TEST_ENV_INVALID").unwrap_err();
        assert!(matches!(
            err,
            EnvParseError::InvalidValue {
                ref key,
                ref value,
                expected: "usize",
                ..
            } if key == "TEST_ENV_INVALID" && value == "abc"
        ));
        assert!(err.to_string().contains("invalid digit found in string"));
    }

    #[test]
    fn env_var_or_uses_default_when_missing() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_DEFAULT"]);
        guard.remove("TEST_ENV_DEFAULT");

        assert_eq!(env_var_or::<usize>("TEST_ENV_DEFAULT", 42).unwrap(), 42);
    }

    #[test]
    fn env_bool_accepts_supported_true_and_false_values() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_BOOL"]);

        for raw in ["1", "true", "TRUE", "yes", "Y", "on"] {
            guard.set("TEST_ENV_BOOL", raw);
            assert_eq!(env_bool("TEST_ENV_BOOL").unwrap(), Some(true), "raw={raw}");
        }

        for raw in ["0", "false", "FALSE", "no", "N", "off"] {
            guard.set("TEST_ENV_BOOL", raw);
            assert_eq!(env_bool("TEST_ENV_BOOL").unwrap(), Some(false), "raw={raw}");
        }
    }

    #[test]
    fn env_bool_reports_invalid_values() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_BOOL_INVALID"]);
        guard.set("TEST_ENV_BOOL_INVALID", "maybe");

        let err = env_bool("TEST_ENV_BOOL_INVALID").unwrap_err();
        assert!(matches!(
            err,
            EnvParseError::InvalidValue {
                ref key,
                ref value,
                expected: "bool",
                ..
            } if key == "TEST_ENV_BOOL_INVALID" && value == "maybe"
        ));
    }

    #[test]
    fn env_bool_or_uses_default_when_missing() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_BOOL_DEFAULT"]);
        guard.remove("TEST_ENV_BOOL_DEFAULT");

        assert!(env_bool_or("TEST_ENV_BOOL_DEFAULT", true).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn env_var_reports_non_unicode_values_as_invalid() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let _lock = env_lock();
        let guard = EnvGuard::new(&["TEST_ENV_NON_UNICODE"]);
        guard.set_os(
            "TEST_ENV_NON_UNICODE",
            OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
        );

        let err = env_var::<usize>("TEST_ENV_NON_UNICODE").unwrap_err();
        assert!(matches!(
            err,
            EnvParseError::InvalidValue {
                ref key,
                expected: "usize",
                ..
            } if key == "TEST_ENV_NON_UNICODE"
        ));
        assert!(err.to_string().contains("not valid UTF-8"));
    }
}
