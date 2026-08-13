// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::observability_payload::{
    LoggingAttribute, LoggingEventName, LoggingEvidence, LoggingLevel, LoggingOccurrence,
    LoggingSchemaError,
};
use obzenflow_runtime::stages::observer::{ObserverDiagnostic, ObserverEvidence, ObserverReport};

/// Payload-opaque logging observer configuration materialised once per stage.
#[derive(Debug, Clone)]
pub struct LoggingMiddleware {
    event: LoggingEventName,
    level: LoggingLevel,
    attributes: Vec<LoggingAttribute>,
    trace_mirror: bool,
}

impl LoggingMiddleware {
    pub fn new(
        event: LoggingEventName,
        level: LoggingLevel,
        attributes: Vec<LoggingAttribute>,
        trace_mirror: bool,
    ) -> Result<Self, LoggingSchemaError> {
        LoggingEvidence::validate_attributes(&attributes)?;
        Ok(Self {
            event,
            level,
            attributes,
            trace_mirror,
        })
    }

    pub(super) fn report(&self, occurrence: LoggingOccurrence) -> ObserverReport {
        let Ok(evidence) = LoggingEvidence::new(
            self.event.clone(),
            self.level,
            occurrence,
            self.attributes.clone(),
        ) else {
            return ObserverReport::empty();
        };
        let local_trace = self
            .trace_mirror
            .then(|| evidence.body().map(str::to_string))
            .flatten();
        let diagnostic = ObserverDiagnostic::new(ObserverEvidence::Logging(evidence));
        let diagnostic = match local_trace {
            Some(body) => diagnostic.with_local_trace(self.level, body),
            None => diagnostic,
        };
        ObserverReport::empty().with_diagnostic(diagnostic)
    }
}
