// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115b: middleware hook binder.
//!
//! The binder is the only layer that sees both the adapter-owned carrier
//! (`MiddlewareSurfaceAttachment`, `MiddlewareOrigin`, ...) and the
//! runtime/infra neutral boundary seams. It maps DSL resolution provenance into
//! the adapter-owned origin, calls `MiddlewareFactory::materialize`, and hands
//! only neutral seams inward (a composed source boundary, a completion gate).

use crate::middleware_resolution::MiddlewareSource;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    MiddlewareAttachmentRequest, MiddlewareFactory, MiddlewareMaterializationContext,
    MiddlewareOrigin, MiddlewareSurface, MiddlewareSurfaceAttachment, ProtectedUnit,
    ProtectedUnitId, SourcePolicy, SourcePollSurface, SourcePollUnitId,
};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::source::strategies::CompletionGate;
use std::sync::Arc;

/// Map the DSL's middleware resolution provenance into the adapter-owned
/// `MiddlewareOrigin`, dropping the DSL-only `overrode_config` detail so adapter
/// APIs never depend on DSL resolution types.
pub(crate) fn middleware_origin_from_source(source: &MiddlewareSource) -> MiddlewareOrigin {
    match source {
        MiddlewareSource::Flow => MiddlewareOrigin::Flow,
        MiddlewareSource::Stage => MiddlewareOrigin::Stage,
        MiddlewareSource::StageOverride {
            family_label,
            flow_label,
            stage_label,
            ..
        } => MiddlewareOrigin::StageOverride {
            family_label: family_label.clone(),
            flow_label: flow_label.clone(),
            stage_label: stage_label.clone(),
        },
    }
}

/// The pieces destructured from one control middleware's `SourcePoll`
/// attachment: the composable source policy and the optional completion-gate
/// companion.
pub(crate) struct SourcePollBinding {
    pub policy: Arc<dyn SourcePolicy>,
    pub completion_gate: Option<Arc<dyn CompletionGate>>,
}

/// Materialize one hook-bound control middleware onto the source-poll surface,
/// returning the neutral pieces the descriptor wires into source runtime config.
pub(crate) fn materialize_source_poll(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    origin: &MiddlewareOrigin,
) -> Result<SourcePollBinding, String> {
    let surface = MiddlewareSurface::SourcePoll(SourcePollSurface {
        stage_id: config.stage_id,
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
    };
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin,
    };
    let ctx = MiddlewareMaterializationContext {
        config,
        control_middleware,
    };
    match factory
        .materialize(request, &ctx)
        .map_err(|e| e.to_string())?
    {
        MiddlewareSurfaceAttachment::SourcePoll(attachment) => Ok(SourcePollBinding {
            policy: attachment.policy,
            completion_gate: attachment.completion_gate,
        }),
        _ => Err(format!(
            "binder expected a SourcePoll attachment from middleware '{}'",
            factory.label()
        )),
    }
}
