// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::TimingMiddleware;
use crate::middleware::{
    validate_attachment_request, ControlMiddlewareRole, Middleware, MiddlewareAttachmentRequest,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareOverrideKey, MiddlewarePlanContribution,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, TopologyMiddlewareConfigSlot,
};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::ObserverCommitError;
use std::sync::Arc;

/// Override-key family for timing observer middleware.
pub struct TimingFamily;

/// Factory for creating `TimingMiddleware` instances with stage context.
pub struct TimingMiddlewareFactory;

impl Default for TimingMiddlewareFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl TimingMiddlewareFactory {
    pub fn new() -> Self {
        Self
    }
}

impl MiddlewareFactory for TimingMiddlewareFactory {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<TimingFamily>("timing")
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
        config: &StageConfig,
        _control_middleware: std::sync::Arc<
            crate::middleware::control::ControlMiddlewareAggregator,
        >,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        Ok(Box::new(TimingMiddleware::new(&config.name)))
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer_with_family(
            self.label(),
            self.override_key().family_label(),
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Handler,
                MiddlewareSurfaceKind::Stateful,
                MiddlewareSurfaceKind::Join,
                MiddlewareSurfaceKind::OutputCommit,
            ],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &crate::middleware::MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let declaration = self.declaration();
        validate_attachment_request(&declaration, &request).map_err(|err| {
            crate::middleware::MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                err,
            )
        })?;
        let observer = Arc::new(TimingMiddleware::new(&context.config.name));
        match request.surface.kind() {
            MiddlewareSurfaceKind::SourcePoll => {
                Ok(MiddlewareSurfaceAttachment::SourcePollObserver(observer))
            }
            MiddlewareSurfaceKind::Handler => {
                Ok(MiddlewareSurfaceAttachment::HandlerObserver(observer))
            }
            MiddlewareSurfaceKind::Stateful => {
                Ok(MiddlewareSurfaceAttachment::StatefulObserver(observer))
            }
            MiddlewareSurfaceKind::Join => Ok(MiddlewareSurfaceAttachment::JoinObserver(observer)),
            MiddlewareSurfaceKind::OutputCommit => {
                Ok(MiddlewareSurfaceAttachment::OutputCommitObserver(observer))
            }
            surface => Err(
                crate::middleware::MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    ObserverCommitError::new(format!(
                        "unsupported timing observer surface {surface:?}"
                    )),
                ),
            ),
        }
    }
}

/// Convenience function to create a timing middleware factory.
pub fn timing() -> Box<dyn MiddlewareFactory> {
    Box::new(TimingMiddlewareFactory::new())
}
