// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Authoring factory for the service-level indicator observer (FLOWIP-115f).
//!
//! `indicator()` is the generic builder; `latency()` is the convenience
//! constructor for the only implemented kind. The factory is hook-bound: it
//! declares the handler observer surface and materializes a `HandlerObserver`.

use super::{IndicatorConfig, IndicatorMiddleware};
use crate::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_core::event::payloads::observability_payload::IndicatorKind;
use obzenflow_runtime::stages::observer::ObserverCommitError;
use serde_json::json;
use std::sync::Arc;
use thiserror::Error;

/// Override-key family for indicator observer middleware.
pub struct IndicatorFamily;

/// Why an indicator's identity is invalid. The `indicator()` factory is
/// fail-closed: a sample with a missing or blank operation/indicator name is
/// unusable evidence (a read-side consumer cannot join it), so the attachment is
/// rejected at flow-build time rather than silently journalling empty strings.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IndicatorConfigError {
    #[error("indicator middleware requires an operation name; call .operation(..)")]
    MissingOperation,
    #[error("indicator middleware operation name must not be blank")]
    BlankOperation,
    #[error("indicator middleware requires an indicator name; call .indicator(..)")]
    MissingIndicator,
    #[error("indicator middleware indicator name must not be blank")]
    BlankIndicator,
}

/// Fluent authoring factory for the service-level indicator observer.
pub struct IndicatorMiddlewareFactory {
    label: &'static str,
    config: IndicatorConfig,
}

impl Default for IndicatorMiddlewareFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl IndicatorMiddlewareFactory {
    /// Generic indicator factory; `kind` defaults to `Latency`.
    pub fn new() -> Self {
        Self {
            label: "indicator",
            config: IndicatorConfig::default(),
        }
    }

    /// Convenience preset for the latency kind, labelled `latency`.
    fn latency_preset() -> Self {
        Self {
            label: "latency",
            config: IndicatorConfig {
                kind: IndicatorKind::Latency,
                ..IndicatorConfig::default()
            },
        }
    }

    /// Name the operation being measured, e.g. `"payment.authorization"`.
    pub fn operation(mut self, operation: impl Into<String>) -> Self {
        self.config.operation = Some(operation.into());
        self
    }

    /// Select the indicator kind. Only [`IndicatorKind::Latency`] is implemented.
    pub fn kind(mut self, kind: IndicatorKind) -> Self {
        self.config.kind = kind;
        self
    }

    /// Name the indicator within the operation, e.g. `"authorization.latency"`.
    pub fn indicator(mut self, indicator: impl Into<String>) -> Self {
        self.config.indicator = Some(indicator.into());
        self
    }

    /// Add a static tag carried on every sample.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.tags.push((key.into(), value.into()));
        self
    }

    /// Reject a missing or blank operation/indicator name. Called by
    /// `materialize` so a nameless indicator fails the flow build (fail-closed)
    /// rather than journalling empty-string identity.
    pub(super) fn validated_identity(&self) -> Result<(), IndicatorConfigError> {
        match self.config.operation.as_deref() {
            None => return Err(IndicatorConfigError::MissingOperation),
            Some(operation) if operation.trim().is_empty() => {
                return Err(IndicatorConfigError::BlankOperation)
            }
            Some(_) => {}
        }
        match self.config.indicator.as_deref() {
            None => return Err(IndicatorConfigError::MissingIndicator),
            Some(indicator) if indicator.trim().is_empty() => {
                return Err(IndicatorConfigError::BlankIndicator)
            }
            Some(_) => {}
        }
        Ok(())
    }
}

impl MiddlewareFactory for IndicatorMiddlewareFactory {
    fn label(&self) -> &'static str {
        self.label
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<IndicatorFamily>(self.label)
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        Some(json!({
            "kind": serde_json::to_value(self.config.kind).ok(),
            "operation": self.config.operation,
            "indicator": self.config.indicator,
            "tags": self.config.tags.iter().map(|(key, value)| {
                json!({ "key": key, "value": value })
            }).collect::<Vec<_>>(),
        }))
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        // Latency is a handler-bracket measurement; the value-preserving
        // processing-time stamp is applied by the runtime output committer, not
        // an observer.
        MiddlewareDeclaration::observer_with_family(
            self.label(),
            self.override_key().family_label(),
            vec![MiddlewareSurfaceKind::Handler],
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
        self.validated_identity().map_err(|err| {
            MiddlewareFactoryError::invalid_configuration(self.label(), &context.config.name, err)
        })?;
        let observer = Arc::new(IndicatorMiddleware::with_config(self.config.clone()));
        match request.surface.kind() {
            MiddlewareSurfaceKind::Handler => {
                Ok(MiddlewareSurfaceAttachment::handler_observer(observer))
            }
            surface => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                ObserverCommitError::new(format!(
                    "unsupported indicator observer surface {surface:?}"
                )),
            )),
        }
    }
}

/// Create a generic service-level indicator observer factory (FLOWIP-115f).
///
/// ```ignore
/// indicator()
///     .operation("payment.authorization")
///     .kind(IndicatorKind::Latency)
///     .indicator("authorization.latency")
///     .tag("dependency", "payment_gateway")
/// ```
pub fn indicator() -> IndicatorMiddlewareFactory {
    IndicatorMiddlewareFactory::new()
}

/// Convenience constructor for a latency indicator, equivalent to
/// `indicator().kind(IndicatorKind::Latency)`.
pub fn latency() -> IndicatorMiddlewareFactory {
    IndicatorMiddlewareFactory::latency_preset()
}
