// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Failure to produce a portable endpoint response.

use std::error::Error;
use std::fmt;

/// An endpoint could not produce its response.
///
/// Intentional HTTP outcomes, including refusals, belong in `ManagedResponse`.
/// The managed host renders this failure as a controlled 500. The static context
/// is suitable for diagnostics; arbitrary source contents are never included in
/// `Display` or `Debug` and must not be copied into a response.
pub struct EndpointError {
    context: &'static str,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl EndpointError {
    pub const fn new(context: &'static str) -> Self {
        Self {
            context,
            source: None,
        }
    }

    pub fn with_source(context: &'static str, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            context,
            source: Some(Box::new(source)),
        }
    }

    pub const fn context(&self) -> &'static str {
        self.context
    }
}

impl fmt::Display for EndpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Endpoint failed: {}", self.context)
    }
}

impl fmt::Debug for EndpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndpointError")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

impl Error for EndpointError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_redact_source_but_preserve_error_chain() {
        let error = EndpointError::with_source(
            "Serialising response",
            std::io::Error::other("private-source-sentinel"),
        );
        assert!(!format!("{error}").contains("private-source-sentinel"));
        assert!(!format!("{error:?}").contains("private-source-sentinel"));
        assert_eq!(
            error.source().unwrap().to_string(),
            "private-source-sentinel"
        );
    }
}
