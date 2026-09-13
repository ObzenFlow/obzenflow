// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Existing system-event wire mappings. No storage or transport access.

use super::middleware::MiddlewareSseState;
use super::{contracts::attach_contract_boundary_aliases, journal_frame, ContractBoundaryAliases};
use obzenflow_core::event::{event_envelope::SystemEventEnvelope, SystemEvent};
use obzenflow_core::web::SseFrame;

/// Map a SystemEvent into an SSE event with JSON payload
pub(super) fn map_system_event_to_sse(
    envelope: &SystemEventEnvelope,
    middleware_state: &mut MiddlewareSseState,
    contract_boundary_aliases: &ContractBoundaryAliases,
) -> Option<SseFrame> {
    use obzenflow_core::event::system_event::StageLifecycleEvent;
    use obzenflow_core::event::SystemEventType;
    use serde_json::json;

    let event: &SystemEvent = &envelope.event;
    let id_str = event.id.to_string();
    let vector_clock_value = serde_json::to_value(&envelope.vector_clock).ok();
    middleware_state.last_vector_clock = Some(envelope.vector_clock.clone());

    match &event.event {
        SystemEventType::SourceCleanupFailed {
            stage_id,
            stage_name,
            error,
        } => {
            let mut data = json!({
                "system_event_type": "source_cleanup_failed",
                "stage_id": stage_id.to_string(),
                "stage_name": stage_name,
                "error": error,
                "timestamp_ms": event.timestamp,
            });
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }
            Some(journal_frame(event.id, "source_cleanup_failed", data))
        }
        SystemEventType::StageLifecycle {
            stage_id,
            event: lifecycle,
        } => {
            let (event_type, metrics_value, error, recoverable, reason) = match lifecycle {
                StageLifecycleEvent::Running => ("stage_running", None, None, None, None),
                StageLifecycleEvent::Draining { metrics } => (
                    "stage_draining",
                    metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
                    None,
                    None,
                    None,
                ),
                StageLifecycleEvent::Drained => ("stage_drained", None, None, None, None),
                StageLifecycleEvent::Completed { metrics } => (
                    "stage_completed",
                    metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
                    None,
                    None,
                    None,
                ),
                StageLifecycleEvent::Cancelled { reason, metrics } => (
                    "stage_cancelled",
                    metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
                    None,
                    None,
                    Some(reason.clone()),
                ),
                StageLifecycleEvent::Failed {
                    error,
                    recoverable,
                    metrics,
                    ..
                } => (
                    "stage_failed",
                    metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()),
                    Some(error.clone()),
                    *recoverable,
                    None,
                ),
            };

            let mut data = json!({
                "system_event_type": "stage_lifecycle",
                "event_type": event_type,
                "stage_id": stage_id.to_string(),
                "timestamp_ms": event.timestamp,
            });

            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            if let Some(m) = metrics_value {
                data["metrics"] = m;
            }
            if let Some(err) = error {
                data["error"] = serde_json::Value::String(err);
            }
            if let Some(r) = reason {
                data["reason"] = serde_json::Value::String(r);
            }
            if let Some(rec) = recoverable {
                data["recoverable"] = serde_json::Value::Bool(rec);
            }

            Some(journal_frame(event.id, "stage_lifecycle", data))
        }
        SystemEventType::PipelineLifecycle(pipeline_event) => match pipeline_event {
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Starting => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_starting",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::ReadyForRun {
                stage_count,
            } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_ready_for_run",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(count) = stage_count {
                    data["stage_count"] = json!(count);
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Running {
                stage_count,
            } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_running",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(count) = stage_count {
                    data["stage_count"] = json!(count);
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::StopAdmitted {
                admission,
            } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_stop_admitted",
                    "timestamp_ms": event.timestamp,
                    "admission": admission,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::NotStarted => {
                let mut data = json!({"system_event_type": "pipeline_lifecycle", "event_type": "flow_not_started", "timestamp_ms": event.timestamp});
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Draining { metrics } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_draining",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(m) = metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()) {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::AllStagesCompleted {
                metrics,
            } => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_stages_completed",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(m) = metrics.as_ref().and_then(|m| serde_json::to_value(m).ok()) {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Drained => {
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_drained",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Completed {
                duration_ms,
                metrics,
            } => {
                let metrics_value = serde_json::to_value(metrics).ok();
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_completed",
                    "timestamp_ms": event.timestamp,
                    "duration_ms": duration_ms,
                });
                if let Some(m) = metrics_value {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Failed {
                reason,
                duration_ms,
                metrics,
                failure_cause,
            } => {
                let metrics_value = metrics.as_ref().and_then(|m| serde_json::to_value(m).ok());
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_failed",
                    "timestamp_ms": event.timestamp,
                    "reason": reason,
                    "duration_ms": duration_ms,
                });
                if let Some(cause) = failure_cause {
                    if let Ok(cause_value) = serde_json::to_value(cause) {
                        data["failure_cause"] = cause_value;
                    }
                }
                if let Some(m) = metrics_value {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
            obzenflow_core::event::system_event::PipelineLifecycleEvent::Cancelled {
                reason,
                duration_ms,
                metrics,
                failure_cause,
            } => {
                let metrics_value = metrics.as_ref().and_then(|m| serde_json::to_value(m).ok());
                let mut data = json!({
                    "system_event_type": "pipeline_lifecycle",
                    "event_type": "flow_cancelled",
                    "timestamp_ms": event.timestamp,
                    "reason": reason,
                    "duration_ms": duration_ms,
                });
                if let Some(cause) = failure_cause {
                    if let Ok(cause_value) = serde_json::to_value(cause) {
                        data["failure_cause"] = cause_value;
                    }
                }
                if let Some(m) = metrics_value {
                    data["metrics"] = m;
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "flow_lifecycle", data))
            }
        },
        SystemEventType::ReplayLifecycle(replay_event) => match replay_event {
            obzenflow_core::event::ReplayLifecycleEvent::Started {
                archive_path,
                archive_flow_id,
                archive_status,
                archive_status_derivation,
                allow_incomplete,
                source_stages,
            } => {
                let mut data = json!({
                    "system_event_type": "replay_lifecycle",
                    "event_type": "replay_started",
                    "timestamp_ms": event.timestamp,
                    "archive_path": archive_path,
                    "archive_flow_id": archive_flow_id,
                    "archive_status": archive_status,
                    "archive_status_derivation": archive_status_derivation,
                    "allow_incomplete": allow_incomplete,
                    "source_stages": source_stages,
                });
                if let Some(stage_id) = event.writer_id.as_stage() {
                    data["stage_id"] = json!(stage_id.to_string());
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "replay_lifecycle", data))
            }
            obzenflow_core::event::ReplayLifecycleEvent::Completed {
                replayed_count,
                skipped_count,
                duration_ms,
                synthesized_eof_kind,
            } => {
                let mut data = json!({
                    "system_event_type": "replay_lifecycle",
                    "event_type": "replay_completed",
                    "timestamp_ms": event.timestamp,
                    "replayed_count": replayed_count,
                    "skipped_count": skipped_count,
                    "duration_ms": duration_ms,
                });
                if let Some(kind) = synthesized_eof_kind {
                    data["synthesized_eof_kind"] = json!(kind);
                }
                if let Some(stage_id) = event.writer_id.as_stage() {
                    data["stage_id"] = json!(stage_id.to_string());
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "replay_lifecycle", data))
            }
            obzenflow_core::event::ReplayLifecycleEvent::ResumedLive {
                archive_flow_id,
                replayed_count,
                generation,
            } => {
                let mut data = json!({
                    "system_event_type": "replay_lifecycle",
                    "event_type": "resumed_live",
                    "timestamp_ms": event.timestamp,
                    "archive_flow_id": archive_flow_id,
                    "replayed_count": replayed_count,
                    "generation": generation,
                });
                if let Some(stage_id) = event.writer_id.as_stage() {
                    data["stage_id"] = json!(stage_id.to_string());
                }
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "replay_lifecycle", data))
            }
        },
        SystemEventType::MiddlewareLifecycle {
            stage_id,
            stage_name,
            flow_id,
            flow_name,
            origin,
            middleware,
        } => {
            middleware_state.observe_middleware_metadata(
                *stage_id,
                stage_name.as_deref(),
                flow_id.as_deref(),
                flow_name.as_deref(),
                Some(&envelope.vector_clock),
            );

            let mut data = json!({
                "system_event_type": "middleware_lifecycle",
                "stage_id": stage_id.to_string(),
                "timestamp_ms": event.timestamp,
                "origin": {
                    "event_id": origin.event_id.to_string(),
                    "writer_key": origin.writer_key,
                    "seq": origin.seq,
                },
                "revision": origin.seq,
            });

            if let Some(name) = stage_name {
                data["stage_name"] = json!(name);
            }
            if let Some(fid) = flow_id {
                data["flow_id"] = json!(fid);
            }
            if let Some(fname) = flow_name {
                data["flow_name"] = json!(fname);
            }
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            if let Some(payload) =
                middleware_state.project_middleware_event(*stage_id, origin.seq.0, middleware)
            {
                if let Some(payload_obj) = payload.as_object() {
                    for (key, value) in payload_obj {
                        data[key] = value.clone();
                    }
                }
            } else {
                return Some(SseFrame::comment("unsupported_middleware_event_skipped"));
            }

            Some(journal_frame(event.id, "middleware_lifecycle", data))
        }
        SystemEventType::ContractStatus {
            upstream,
            reader,
            selected_event_type,
            feed_role,
            pass,
            reader_seq,
            advertised_writer_seq,
            reason,
        } => {
            let mut data = json!({
                "system_event_type": "contract_status",
                "upstream_stage_id": upstream.to_string(),
                "reader_stage_id": reader.to_string(),
                "pass": pass,
                "timestamp_ms": event.timestamp,
            });

            if let Some(seq) = reader_seq {
                data["reader_seq"] = serde_json::json!(seq);
            }
            if let Some(seq) = advertised_writer_seq {
                data["advertised_writer_seq"] = serde_json::json!(seq);
            }
            if let Some(cause) = reason {
                data["reason"] = serde_json::json!(cause);
            }
            if let Some(selected_event_type) = selected_event_type {
                data["selected_event_type"] = serde_json::json!(selected_event_type);
            }
            if let Some(feed_role) = feed_role {
                data["feed_role"] = serde_json::json!(feed_role);
            }
            attach_contract_boundary_aliases(
                &mut data,
                contract_boundary_aliases,
                *upstream,
                *reader,
            );
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            let sse_event_name = if *pass {
                "contract_status"
            } else {
                "contract_violation"
            };

            Some(journal_frame(event.id, sse_event_name, data))
        }
        SystemEventType::ContractResult {
            upstream,
            reader,
            selected_event_type,
            feed_role,
            contract_name,
            status,
            cause,
            reader_seq,
            advertised_writer_seq,
        } => {
            let mut data = json!({
                "system_event_type": "contract_result",
                "upstream_stage_id": upstream.to_string(),
                "reader_stage_id": reader.to_string(),
                "contract_name": contract_name,
                "status": status,
                "timestamp_ms": event.timestamp,
            });

            if let Some(cause) = cause {
                data["cause"] = serde_json::json!(cause);
            }
            if let Some(seq) = reader_seq {
                data["reader_seq"] = serde_json::json!(seq);
            }
            if let Some(seq) = advertised_writer_seq {
                data["advertised_writer_seq"] = serde_json::json!(seq);
            }
            if let Some(selected_event_type) = selected_event_type {
                data["selected_event_type"] = serde_json::json!(selected_event_type);
            }
            if let Some(feed_role) = feed_role {
                data["feed_role"] = serde_json::json!(feed_role);
            }
            attach_contract_boundary_aliases(
                &mut data,
                contract_boundary_aliases,
                *upstream,
                *reader,
            );
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            Some(journal_frame(event.id, "contract_result", data))
        }
        SystemEventType::EdgeLiveness {
            upstream,
            reader,
            state,
            idle_ms,
            last_reader_seq,
            last_event_id,
        } => {
            let mut data = json!({
                "system_event_type": "edge_liveness",
                "upstream_stage_id": upstream.to_string(),
                "reader_stage_id": reader.to_string(),
                "state": state,
                "idle_ms": idle_ms,
                "timestamp_ms": event.timestamp,
            });

            if let Some(seq) = last_reader_seq {
                data["last_reader_seq"] = serde_json::json!(seq);
            }
            if let Some(event_id) = last_event_id {
                data["last_event_id"] = serde_json::json!(event_id.to_string());
            }
            if let Some(vc) = &vector_clock_value {
                data["vector_clock"] = vc.clone();
            }

            Some(journal_frame(event.id, "edge_liveness", data))
        }
        SystemEventType::StageHeartbeat { .. } => None,
        SystemEventType::MetricsCoordination(metrics_event) => match metrics_event {
            obzenflow_core::event::system_event::MetricsCoordinationEvent::Exported {
                watermark,
            } => {
                let mut data = json!({
                    "timestamp_ms": event.timestamp,
                    "watermark": watermark,
                    "export_id": id_str,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "metrics_watermark", data))
            }
            obzenflow_core::event::system_event::MetricsCoordinationEvent::Ready => {
                let mut data = json!({
                    "system_event_type": "metrics_coordination",
                    "event_type": "metrics_ready",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "metrics_coordination", data))
            }
            obzenflow_core::event::system_event::MetricsCoordinationEvent::DrainRequested => {
                let mut data = json!({
                    "system_event_type": "metrics_coordination",
                    "event_type": "metrics_drain_requested",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "metrics_coordination", data))
            }
            obzenflow_core::event::system_event::MetricsCoordinationEvent::Drained => {
                let mut data = json!({
                    "system_event_type": "metrics_coordination",
                    "event_type": "metrics_drained",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "metrics_coordination", data))
            }
            obzenflow_core::event::system_event::MetricsCoordinationEvent::Shutdown => {
                let mut data = json!({
                    "system_event_type": "metrics_coordination",
                    "event_type": "metrics_shutdown",
                    "timestamp_ms": event.timestamp,
                });
                if let Some(vc) = &vector_clock_value {
                    data["vector_clock"] = vc.clone();
                }
                Some(journal_frame(event.id, "metrics_coordination", data))
            }
        },
        // FLOWIP-093a: keep system-level hosted-surface snapshot facts out of the SSE stream
        // by default to avoid turning /api/flow/events into a high-volume metrics pipe.
        SystemEventType::HttpSurfaceSnapshot { .. } => None,
        // FLOWIP-115d: ingress refusal facts are projected into /metrics, not the
        // SSE lifecycle stream (same treatment as the hosted-surface snapshot).
        SystemEventType::IngressRefusal { .. } => None,
    }
}
