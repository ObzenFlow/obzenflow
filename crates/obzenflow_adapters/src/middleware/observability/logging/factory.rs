// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Authoring factory for the logging observer (FLOWIP-115f).
//!
//! `log()` attaches a logging observer to the surfaces it supports (handler,
//! stateful, join, source poll, and sink delivery). Each hook publishes a
//! journalled `MiddlewareLifecycle::User` evidence row and may also mirror the
//! message to `tracing` for local visibility; the journalled wide event is the
//! source of truth. The factory is hook-bound and has no legacy shell.

use super::LoggingMiddleware;
use crate::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_runtime::stages::observer::ObserverCommitError;
use serde_json::json;
use std::sync::Arc;

/// Override-key family for logging observer middleware.
pub struct LoggingFamily;

/// Fluent authoring factory for the logging observer.
#[derive(Clone)]
pub struct LoggingMiddlewareFactory {
    prefix: Option<String>,
    level: tracing::Level,
}

impl Default for LoggingMiddlewareFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl LoggingMiddlewareFactory {
    pub fn new() -> Self {
        Self {
            prefix: None,
            level: tracing::Level::INFO,
        }
    }

    /// Prefix every journalled and traced message.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Set the tracing level for the local mirror of each evidence row.
    pub fn level(mut self, level: tracing::Level) -> Self {
        self.level = level;
        self
    }

    fn build(&self) -> LoggingMiddleware {
        let middleware = match &self.prefix {
            Some(prefix) => LoggingMiddleware::with_prefix(prefix.clone()),
            None => LoggingMiddleware::new(),
        };
        middleware.with_level(self.level)
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
            "prefix": self.prefix,
            "level": self.level.to_string(),
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
        validate_attachment_request(&declaration, &request).map_err(|err| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, err)
        })?;
        let observer = Arc::new(self.build());
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

/// Create a logging observer factory (FLOWIP-115f).
///
/// Publishes a journalled `MiddlewareLifecycle::User` evidence row at each
/// supported hook, e.g. on sink delivery for an operational handoff.
pub fn log() -> LoggingMiddlewareFactory {
    LoggingMiddlewareFactory::new()
}
