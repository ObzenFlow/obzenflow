// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Required-name authoring factory for typed logging evidence (FLOWIP-115m).

use super::LoggingMiddleware;
use crate::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_core::event::payloads::observability_payload::{
    LoggingAttribute, LoggingEventName, LoggingLevel,
};
use obzenflow_runtime::stages::observer::ObserverCommitError;
use serde_json::json;
use std::sync::Arc;

/// Override-key family for logging observer middleware.
pub struct LoggingFamily;

/// Fluent authoring factory for one named logging occurrence family.
#[derive(Clone)]
pub struct LoggingMiddlewareFactory {
    event: String,
    level: LoggingLevel,
    tags: Vec<(String, String)>,
    trace_mirror: bool,
}

impl LoggingMiddlewareFactory {
    pub fn new(event: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            level: LoggingLevel::Info,
            tags: Vec::new(),
            trace_mirror: false,
        }
    }

    pub fn level(mut self, level: LoggingLevel) -> Self {
        self.level = level;
        self
    }

    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.push((key.into(), value.into()));
        self
    }

    pub fn trace_mirror(mut self) -> Self {
        self.trace_mirror = true;
        self
    }

    fn build(&self) -> Result<LoggingMiddleware, String> {
        let event = LoggingEventName::new(self.event.clone()).map_err(|error| error.to_string())?;
        let attributes = self
            .tags
            .iter()
            .map(|(key, value)| LoggingAttribute::new(key.clone(), value.clone()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        LoggingMiddleware::new(event, self.level, attributes, self.trace_mirror)
            .map_err(|error| error.to_string())
    }
}

impl MiddlewareFactory for LoggingMiddlewareFactory {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<LoggingFamily>(self.label())
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        Some(json!({
            "event": self.event,
            "level": self.level,
            "tags": self.tags,
            "trace_mirror": self.trace_mirror,
        }))
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer_with_family(
            self.label(),
            self.override_key().family_label(),
            vec![
                MiddlewareSurfaceKind::Handler,
                MiddlewareSurfaceKind::Stateful,
                MiddlewareSurfaceKind::Join,
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::SinkDelivery,
            ],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let declaration = self.declaration();
        validate_attachment_request(&declaration, &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        let observer = Arc::new(self.build().map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                ObserverCommitError::new(error),
            )
        })?);
        match request.surface.kind() {
            MiddlewareSurfaceKind::Handler => {
                Ok(MiddlewareSurfaceAttachment::handler_observer(observer))
            }
            MiddlewareSurfaceKind::Stateful => {
                Ok(MiddlewareSurfaceAttachment::stateful_observer(observer))
            }
            MiddlewareSurfaceKind::Join => Ok(MiddlewareSurfaceAttachment::join_observer(observer)),
            MiddlewareSurfaceKind::SourcePoll => {
                Ok(MiddlewareSurfaceAttachment::source_poll_observer(observer))
            }
            MiddlewareSurfaceKind::SinkDelivery => Ok(
                MiddlewareSurfaceAttachment::sink_delivery_observer(observer),
            ),
            surface => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                ObserverCommitError::new(format!(
                    "unsupported logging observer surface {surface:?}"
                )),
            )),
        }
    }
}

/// Create a logging observer with required durable semantic identity.
pub fn log_event(name: impl Into<String>) -> LoggingMiddlewareFactory {
    LoggingMiddlewareFactory::new(name)
}
