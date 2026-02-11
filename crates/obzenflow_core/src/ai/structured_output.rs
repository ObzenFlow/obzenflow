// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::StructuredOutputError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum StructuredOutputSchema {
    Text(String),
    Json(Value),
}

pub type ValidationHook<T> = Arc<dyn Fn(&T) -> Result<(), String> + Send + Sync + 'static>;

/// Spec for parsing AI JSON output into a Rust type with explicit failure modes.
pub struct StructuredOutputSpec<T> {
    pub schema: StructuredOutputSchema,
    pub prompt_prefix: Option<String>,
    validator: Option<ValidationHook<T>>,
}

impl<T> std::fmt::Debug for StructuredOutputSpec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructuredOutputSpec")
            .field("schema", &self.schema)
            .field("prompt_prefix", &self.prompt_prefix)
            .field("has_validator", &self.validator.is_some())
            .finish()
    }
}

impl<T> Clone for StructuredOutputSpec<T> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            prompt_prefix: self.prompt_prefix.clone(),
            validator: self.validator.clone(),
        }
    }
}

impl<T> StructuredOutputSpec<T>
where
    T: DeserializeOwned,
{
    pub fn from_text_schema(schema: impl Into<String>) -> Self {
        Self {
            schema: StructuredOutputSchema::Text(schema.into()),
            prompt_prefix: None,
            validator: None,
        }
    }

    pub fn from_json_schema(schema: Value) -> Self {
        Self {
            schema: StructuredOutputSchema::Json(schema),
            prompt_prefix: None,
            validator: None,
        }
    }

    pub fn with_prompt_prefix(mut self, prompt_prefix: impl Into<String>) -> Self {
        self.prompt_prefix = Some(prompt_prefix.into());
        self
    }

    pub fn with_validator<F>(mut self, validator: F) -> Self
    where
        F: Fn(&T) -> Result<(), String> + Send + Sync + 'static,
    {
        self.validator = Some(Arc::new(validator));
        self
    }

    pub fn parse_json_str(&self, raw_json: &str) -> Result<T, StructuredOutputError> {
        let value = serde_json::from_str::<Value>(raw_json).map_err(|err| {
            StructuredOutputError::InvalidJson {
                message: err.to_string(),
            }
        })?;
        self.parse_value(value)
    }

    pub fn parse_value(&self, value: Value) -> Result<T, StructuredOutputError> {
        let parsed: T = serde_json::from_value(value).map_err(|err| {
            StructuredOutputError::Deserialization {
                message: err.to_string(),
            }
        })?;

        if let Some(validator) = &self.validator {
            validator(&parsed).map_err(|msg| StructuredOutputError::Validation { message: msg })?;
        }

        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TicketSummary {
        category: String,
        confidence: f64,
    }

    #[test]
    fn structured_output_parse_value_success() {
        let spec = StructuredOutputSpec::<TicketSummary>::from_json_schema(json!({
            "type": "object",
            "required": ["category", "confidence"],
            "properties": {
                "category": { "type": "string" },
                "confidence": { "type": "number" }
            }
        }));

        let parsed = spec
            .parse_value(json!({"category": "billing", "confidence": 0.92}))
            .expect("valid JSON should parse");

        assert_eq!(
            parsed,
            TicketSummary {
                category: "billing".to_string(),
                confidence: 0.92
            }
        );
    }

    #[test]
    fn structured_output_parse_json_str_reports_invalid_json() {
        let spec = StructuredOutputSpec::<TicketSummary>::from_text_schema("category + confidence");
        let err = spec
            .parse_json_str("{ this is not json }")
            .expect_err("invalid JSON should error");

        assert!(matches!(err, StructuredOutputError::InvalidJson { .. }));
    }

    #[test]
    fn structured_output_validation_hook_reports_validation_error() {
        let spec = StructuredOutputSpec::<TicketSummary>::from_text_schema("summary")
            .with_validator(|summary| {
                if summary.confidence < 0.0 || summary.confidence > 1.0 {
                    return Err("confidence must be in [0, 1]".to_string());
                }
                Ok(())
            });

        let err = spec
            .parse_value(json!({"category": "billing", "confidence": 1.4}))
            .expect_err("validation should fail");

        assert!(matches!(err, StructuredOutputError::Validation { .. }));
    }
}
