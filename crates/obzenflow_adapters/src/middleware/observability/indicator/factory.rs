// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Authoring factory for the service-level indicator observer (FLOWIP-115f).
//!
//! `indicator()` is the generic builder; `latency()` is the convenience
//! constructor for the only implemented kind. The factory is hook-bound: it
//! declares the handler observer surface and materializes a `HandlerObserver`,
//! and its legacy `create()` path fails loudly because an observer has no
//! legacy shell.

use super::{IndicatorConfig, IndicatorMiddleware};
use crate::middleware::{
    validate_attachment_request, ControlMiddlewareRole, Middleware, MiddlewareAttachmentRequest,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewarePlanContribution,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::payloads::observability_payload::IndicatorKind;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::observer::ObserverCommitError;
use serde_json::json;
use std::sync::Arc;

/// Override-key family for indicator observer middleware.
pub struct IndicatorFamily;

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
}

impl MiddlewareFactory for IndicatorMiddlewareFactory {
    fn label(&self) -> &'static str {
        self.label
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<IndicatorFamily>(self.label)
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::None
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        None
    }

    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: std::sync::Arc<
            crate::middleware::control::ControlMiddlewareAggregator,
        >,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        // An indicator is a hook-bound observer with no legacy shell. The DSL
        // placement planner rejects it before reaching here; this is the loud
        // fallback if a caller bypasses the planner.
        Err(MiddlewareFactoryError::not_hook_bound(self.label()))
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
        // processing-time stamp is the framework's built-in timing observer.
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
        let observer = Arc::new(IndicatorMiddleware::with_config(self.config.clone()));
        match request.surface.kind() {
            MiddlewareSurfaceKind::Handler => {
                Ok(MiddlewareSurfaceAttachment::HandlerObserver(observer))
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
