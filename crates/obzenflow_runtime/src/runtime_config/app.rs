// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The config file's `[app]` table (FLOWIP-010t): carried untyped in the
//! immutable snapshot, deserialized into the application's own serde type
//! once per build, and never interpreted by the framework.

use crate::classified_de::{self, ClassifiedError};
use serde::de::{self, DeserializeOwned, Expected, Unexpected};
use serde::Serialize;
use serde_json::{Map, Value};
use std::fmt;
use std::path::{Path, PathBuf};

/// Longer map keys are elided from diagnostics (B7).
const MAX_SHOWN_KEY_BYTES: usize = 64;
/// Name lists in diagnostics stop after this many names (B7, B10).
const MAX_LISTED_NAMES: usize = 8;

/// The `[app]` table as the config file wrote it, and the file it came from.
/// `Debug` shows keys only: a value may be a mistyped secret.
#[derive(Clone, Default, PartialEq)]
pub struct AppTable {
    origin: Option<PathBuf>,
    table: Map<String, Value>,
}

impl AppTable {
    pub fn new(origin: impl Into<PathBuf>, table: Map<String, Value>) -> Self {
        Self {
            origin: Some(origin.into()),
            table,
        }
    }

    pub fn origin(&self) -> Option<&Path> {
        self.origin.as_deref()
    }

    /// An absent table is an empty one.
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Deserialize the table into `T`, and serialize `T` back as evidence.
    pub(crate) fn extract<T: DeserializeOwned + Serialize>(
        &self,
    ) -> Result<(T, Value), ApplicationSettingsError> {
        let table = Value::Object(self.table.clone());
        let settings = classified_de::from_value::<T, ConfigDeError>(&table)
            .map_err(ApplicationSettingsError::Invalid)?;
        let evidence =
            serde_json::to_value(&settings).map_err(|_| ApplicationSettingsError::Unrecordable)?;
        Ok((settings, evidence))
    }

    /// The refusal for a non-empty table that no build extracted (B10).
    pub(crate) fn unextracted(&self) -> ApplicationSettingsError {
        ApplicationSettingsError::Unextracted {
            origin: self.origin.clone(),
            keys: self.table.keys().take(MAX_LISTED_NAMES).cloned().collect(),
            unlisted: self.table.len().saturating_sub(MAX_LISTED_NAMES),
        }
    }
}

impl fmt::Debug for AppTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppTable")
            .field("origin", &self.origin)
            .field("keys", &self.table.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Why a build could not use the `[app]` table. Diagnostics name keys and
/// types, never values (B7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplicationSettingsError {
    /// The table does not fit the application's type.
    Invalid(ConfigDeError),
    /// A build extracts its settings once (B4).
    AlreadyExtracted,
    /// The extracted value has no JSON form to record as evidence.
    Unrecordable,
    /// A non-empty table that no `materialize_with` extracted (B10).
    Unextracted {
        origin: Option<PathBuf>,
        keys: Vec<String>,
        unlisted: usize,
    },
}

impl fmt::Display for ApplicationSettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(error) => error.fmt(f),
            Self::AlreadyExtracted => {
                f.write_str("application settings were already extracted for this build")
            }
            Self::Unrecordable => {
                f.write_str("app: the extracted settings cannot be recorded as run evidence")
            }
            Self::Unextracted {
                origin,
                keys,
                unlisted,
            } => {
                match origin {
                    Some(origin) => write!(f, "app: {} sets ", origin.display())?,
                    None => f.write_str("app: the config file sets ")?,
                }
                for (index, key) in keys.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write_key(f, key)?;
                }
                if *unlisted > 0 {
                    write!(f, " and {unlisted} more")?;
                }
                f.write_str(
                    ", but this flow extracts no application settings; remove [app] or extract \
                     it with FlowDefinition::materialize_with",
                )
            }
        }
    }
}

impl std::error::Error for ApplicationSettingsError {}

/// The config policy of the classified deserializer (B7): every key the
/// operator wrote, the type's expectation and the kind of value found. It
/// never keeps a value, an unknown variant's text or a custom error's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDeError {
    /// Innermost segment first, as the failure unwinds.
    path: Vec<Segment>,
    problem: Problem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Key(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Problem {
    Invalid,
    MissingField,
    UnknownField(&'static [&'static str]),
    DuplicateField,
    UnknownVariant(&'static [&'static str]),
    InvalidType {
        found: &'static str,
        expected: String,
    },
    InvalidValue {
        expected: String,
    },
    InvalidLength {
        expected: String,
    },
}

impl ConfigDeError {
    fn new(problem: Problem) -> Self {
        Self {
            path: Vec::new(),
            problem,
        }
    }

    fn at_key(problem: Problem, key: &str) -> Self {
        Self {
            path: vec![Segment::Key(key.to_string())],
            problem,
        }
    }
}

impl ClassifiedError for ConfigDeError {
    fn within_key(mut self, key: &str, _known: Option<&'static [&'static str]>) -> Self {
        self.path.push(Segment::Key(key.to_string()));
        self
    }

    fn within_index(mut self, index: usize) -> Self {
        self.path.push(Segment::Index(index));
        self
    }
}

impl de::Error for ConfigDeError {
    fn custom<T: fmt::Display>(_message: T) -> Self {
        Self::new(Problem::Invalid)
    }

    fn missing_field(field: &'static str) -> Self {
        Self::at_key(Problem::MissingField, field)
    }

    fn unknown_field(field: &str, expected: &'static [&'static str]) -> Self {
        Self::at_key(Problem::UnknownField(expected), field)
    }

    fn duplicate_field(field: &'static str) -> Self {
        Self::at_key(Problem::DuplicateField, field)
    }

    fn invalid_type(unexpected: Unexpected<'_>, expected: &dyn Expected) -> Self {
        Self::new(Problem::InvalidType {
            found: kind(&unexpected),
            expected: expected.to_string(),
        })
    }

    fn invalid_value(_unexpected: Unexpected<'_>, expected: &dyn Expected) -> Self {
        Self::new(Problem::InvalidValue {
            expected: expected.to_string(),
        })
    }

    fn invalid_length(_len: usize, expected: &dyn Expected) -> Self {
        Self::new(Problem::InvalidLength {
            expected: expected.to_string(),
        })
    }

    fn unknown_variant(_variant: &str, expected: &'static [&'static str]) -> Self {
        Self::new(Problem::UnknownVariant(expected))
    }
}

impl fmt::Display for ConfigDeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("app")?;
        for segment in self.path.iter().rev() {
            match segment {
                Segment::Key(key) => {
                    f.write_str(".")?;
                    write_key(f, key)?;
                }
                Segment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        f.write_str(": ")?;
        match &self.problem {
            Problem::Invalid => f.write_str("invalid value"),
            Problem::MissingField => f.write_str("missing field"),
            Problem::UnknownField([]) => f.write_str("unknown field, there are no fields"),
            Problem::UnknownField(names) => {
                f.write_str("unknown field, expected one of ")?;
                write_names(f, names)
            }
            Problem::DuplicateField => f.write_str("duplicate field"),
            Problem::UnknownVariant([]) => f.write_str("unknown variant, there are no variants"),
            Problem::UnknownVariant(names) => {
                f.write_str("unknown variant, expected one of ")?;
                write_names(f, names)
            }
            Problem::InvalidType { found, expected } => {
                write!(f, "found {found}, expected {expected}")
            }
            Problem::InvalidValue { expected } => write!(f, "invalid value, expected {expected}"),
            Problem::InvalidLength { expected } => write!(f, "invalid length, expected {expected}"),
        }
    }
}

impl std::error::Error for ConfigDeError {}

/// The kind of value found, in TOML's terms; never the value.
fn kind(unexpected: &Unexpected<'_>) -> &'static str {
    match unexpected {
        Unexpected::Bool(_) => "a boolean",
        Unexpected::Unsigned(_) | Unexpected::Signed(_) => "an integer",
        Unexpected::Float(_) => "a float",
        Unexpected::Char(_) => "a character",
        Unexpected::Str(_) => "a string",
        Unexpected::Bytes(_) => "bytes",
        Unexpected::Unit => "an empty value",
        Unexpected::Seq => "an array",
        Unexpected::Map => "a table",
        Unexpected::Enum
        | Unexpected::UnitVariant
        | Unexpected::NewtypeVariant
        | Unexpected::TupleVariant
        | Unexpected::StructVariant => "an enum variant",
        Unexpected::Option | Unexpected::NewtypeStruct | Unexpected::Other(_) => "a value",
    }
}

/// One `[app]` key in a diagnostic, elided past 64 bytes.
fn write_key(f: &mut fmt::Formatter<'_>, key: &str) -> fmt::Result {
    if key.len() > MAX_SHOWN_KEY_BYTES {
        f.write_str("<key longer than 64 bytes>")
    } else {
        f.write_str(&toml_key(key))
    }
}

/// A key as TOML spells it: bare when it can be, quoted otherwise.
pub(crate) fn toml_key(key: &str) -> String {
    let bare = !key.is_empty()
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    if bare {
        key.to_string()
    } else {
        format!("\"{}\"", key.escape_debug())
    }
}

fn write_names(f: &mut fmt::Formatter<'_>, names: &[&str]) -> fmt::Result {
    for (index, name) in names.iter().take(MAX_LISTED_NAMES).enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        write!(f, "`{name}`")?;
    }
    if names.len() > MAX_LISTED_NAMES {
        write!(f, " and {} more", names.len() - MAX_LISTED_NAMES)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::config::SecretRef;
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::BTreeMap;

    const SENTINEL: &str = "sk_live_51HSENTINEL";

    fn table(value: Value) -> AppTable {
        match value {
            Value::Object(map) => AppTable::new("deploy/staging.toml", map),
            other => panic!("not a table: {other}"),
        }
    }

    fn extract<T: DeserializeOwned + Serialize>(value: Value) -> Result<T, String> {
        table(value)
            .extract::<T>()
            .map(|(settings, _)| settings)
            .map_err(|error| {
                let rendered = format!("{error} {error:?}");
                assert!(!rendered.contains(SENTINEL), "{rendered}");
                error.to_string()
            })
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    #[serde(rename_all = "snake_case")]
    enum Mode {
        Live,
        Fixture,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Partner {
        base_url: String,
        #[serde(default)]
        api_key_env: Option<SecretRef>,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Settings {
        accounts_file: String,
        #[serde(default = "default_retries")]
        max_retries: u32,
        #[serde(default)]
        mode: Option<Mode>,
        #[serde(default)]
        partners: BTreeMap<String, Partner>,
        #[serde(default)]
        thresholds: Vec<u32>,
    }

    fn default_retries() -> u32 {
        3
    }

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(try_from = "String")]
    struct Validated(String);

    impl TryFrom<String> for Validated {
        type Error = String;

        fn try_from(text: String) -> Result<Self, String> {
            Err(format!("rejected {text}"))
        }
    }

    #[test]
    fn extraction_applies_defaults_and_records_the_serialized_value() {
        let app = table(json!({
            "accounts_file": "accounts.yaml",
            "partners": {"acme": {"base_url": "https://acme.test", "api_key_env": "ACME_KEY"}},
        }));
        let (settings, evidence) = app.extract::<Settings>().unwrap();
        assert_eq!(settings.max_retries, 3);
        assert_eq!(
            settings.partners["acme"]
                .api_key_env
                .as_ref()
                .unwrap()
                .env_name(),
            "ACME_KEY"
        );
        assert_eq!(
            evidence,
            json!({
                "accounts_file": "accounts.yaml",
                "max_retries": 3,
                "mode": null,
                "partners": {"acme": {"base_url": "https://acme.test", "api_key_env": "ACME_KEY"}},
                "thresholds": [],
            })
        );
    }

    #[test]
    fn an_absent_table_extracts_as_an_empty_one() {
        #[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
        #[serde(default)]
        struct Optional {
            label: Option<String>,
        }
        let (settings, evidence) = AppTable::default().extract::<Optional>().unwrap();
        assert_eq!(settings, Optional::default());
        assert_eq!(evidence, json!({"label": null}));
        assert_eq!(
            AppTable::default()
                .extract::<Settings>()
                .unwrap_err()
                .to_string(),
            "app.accounts_file: missing field"
        );
    }

    #[test]
    fn diagnostics_name_the_key_the_expectation_and_the_kind_found() {
        assert_eq!(
            extract::<Settings>(json!({"acounts_file": "a"})).unwrap_err(),
            "app.acounts_file: unknown field, expected one of `accounts_file`, `max_retries`, \
             `mode`, `partners`, `thresholds`"
        );
        assert_eq!(
            extract::<Settings>(json!({"accounts_file": "a", "max_retries": SENTINEL}))
                .unwrap_err(),
            "app.max_retries: found a string, expected u32"
        );
        assert_eq!(
            extract::<Settings>(json!({"accounts_file": "a", "mode": SENTINEL})).unwrap_err(),
            "app.mode: unknown variant, expected one of `live`, `fixture`"
        );
        assert_eq!(
            extract::<Settings>(json!({
                "accounts_file": "a",
                "partners": {"acme": {"base_url": 7}},
            }))
            .unwrap_err(),
            "app.partners.acme.base_url: found an integer, expected a string"
        );
        assert_eq!(
            extract::<Settings>(json!({"accounts_file": "a", "thresholds": [1, SENTINEL]}))
                .unwrap_err(),
            "app.thresholds[1]: found a string, expected u32"
        );
        assert_eq!(
            extract::<Settings>(json!({
                "accounts_file": "a",
                "partners": {"acme corp": {"base_url": "x", "extra": 1}},
            }))
            .unwrap_err(),
            "app.partners.\"acme corp\".extra: unknown field, expected one of `base_url`, \
             `api_key_env`"
        );
    }

    #[test]
    fn secrets_and_custom_error_text_never_reach_a_diagnostic() {
        assert_eq!(
            extract::<Settings>(json!({
                "accounts_file": "a",
                "partners": {"acme": {"base_url": "x", "api_key_env": SENTINEL}},
            }))
            .unwrap_err(),
            "app.partners.acme.api_key_env: invalid value, expected an environment variable name"
        );

        #[derive(Debug, Deserialize, Serialize)]
        struct Checked {
            token: Validated,
        }
        assert_eq!(
            extract::<Checked>(json!({"token": SENTINEL})).unwrap_err(),
            "app.token: invalid value"
        );
    }

    #[test]
    fn long_map_keys_are_elided_and_name_lists_stop_after_eight() {
        let long = "k".repeat(65);
        let mut partners = Map::new();
        partners.insert(long, json!({"base_url": 1}));
        assert_eq!(
            extract::<Settings>(json!({"accounts_file": "a", "partners": partners})).unwrap_err(),
            "app.partners.<key longer than 64 bytes>.base_url: found an integer, expected a string"
        );

        #[derive(Debug, Deserialize, Serialize)]
        #[serde(deny_unknown_fields)]
        struct Wide {
            a: Option<u8>,
            b: Option<u8>,
            c: Option<u8>,
            d: Option<u8>,
            e: Option<u8>,
            f: Option<u8>,
            g: Option<u8>,
            h: Option<u8>,
            i: Option<u8>,
            j: Option<u8>,
        }
        assert_eq!(
            extract::<Wide>(json!({"z": 1})).unwrap_err(),
            "app.z: unknown field, expected one of `a`, `b`, `c`, `d`, `e`, `f`, `g`, `h` and 2 more"
        );
    }

    #[test]
    fn an_unextracted_table_names_its_file_and_top_level_keys() {
        let app = table(json!({"accounts_file": "a"}));
        assert_eq!(
            app.unextracted().to_string(),
            "app: deploy/staging.toml sets accounts_file, but this flow extracts no application \
             settings; remove [app] or extract it with FlowDefinition::materialize_with"
        );

        let mut keys = Map::new();
        for name in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"] {
            keys.insert(name.to_string(), json!(SENTINEL));
        }
        let wide = AppTable::new("obzenflow.toml", keys);
        let rendered = wide.unextracted().to_string();
        assert!(
            rendered.starts_with("app: obzenflow.toml sets a, b, c, d, e, f, g, h and 2 more, "),
            "{rendered}"
        );
        assert!(!format!("{rendered} {wide:?}").contains(SENTINEL));
    }
}
