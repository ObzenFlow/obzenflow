// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed supervisor report authoring and interpretation. These conversions happen
//! before commitment or project a payload; they never change a committed envelope.

use super::execution_payload::{ExecutionPayload, StageLifecycleFact as Fact};
use super::system_payload::{StageLifecycleEvent as Lifecycle, SystemPayload};

impl ExecutionPayload {
    pub fn from_supervision_report(report: SystemPayload) -> Result<Self, &'static str> {
        Ok(match report {
            SystemPayload::ReplayLifecycle(event) => Self::ReplayLifecycle(event),
            SystemPayload::SupervisorRegistered { descriptor } => {
                Self::SupervisorRegistered { descriptor }
            }
            SystemPayload::SupervisorCommandDiscarded {
                supervisor,
                terminal_state,
                command,
                disposition,
                error,
            } => Self::SupervisorCommandDiscarded {
                supervisor,
                terminal_state,
                command,
                disposition,
                error,
            },
            SystemPayload::SourceCleanupFailed {
                stage_id,
                stage_name,
                error,
            } => Self::SourceCleanupFailed {
                stage_id,
                stage_name,
                error,
            },
            SystemPayload::ContractStatus {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                pass,
                reader_seq,
                advertised_writer_seq,
                reason,
            } => Self::ContractStatus {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                pass,
                reader_seq,
                advertised_writer_seq,
                reason,
            },
            SystemPayload::ContractResult {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                contract_name,
                status,
                cause,
                reader_seq,
                advertised_writer_seq,
            } => Self::ContractResult {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                contract_name,
                status,
                cause,
                reader_seq,
                advertised_writer_seq,
            },
            SystemPayload::IngressRefusal {
                ingress_key,
                stage_id,
                stage_key,
                reason,
                attempt_seq,
                request_count,
                event_count,
                batch_count,
                http_status,
                retry_after_ms_bucket,
            } => Self::IngressRefusal {
                ingress_key,
                stage_id,
                stage_key,
                reason,
                attempt_seq,
                request_count,
                event_count,
                batch_count,
                http_status,
                retry_after_ms_bucket,
            },
            SystemPayload::StageLifecycle { stage_id, event } => {
                Self::StageLifecycle(match event {
                    Lifecycle::Running => Fact::Running { stage_id },
                    Lifecycle::Draining { accounting } => Fact::Draining {
                        stage_id,
                        reason: None,
                        accounting,
                    },
                    Lifecycle::Drained => Fact::Drained {
                        stage_id,
                        events_processed: None,
                    },
                    Lifecycle::Completed { accounting } => Fact::Completed {
                        stage_id,
                        accounting,
                    },
                    Lifecycle::Cancelled { reason, accounting } => Fact::Cancelled {
                        stage_id,
                        reason,
                        accounting,
                    },
                    Lifecycle::Failed {
                        error,
                        recoverable,
                        accounting,
                        causal_event_id,
                    } => Fact::Failed {
                        stage_id,
                        error,
                        recoverable,
                        accounting,
                        causal_event_id,
                    },
                })
            }
            _ => return Err("payload is not a stage supervision report"),
        })
    }

    /// The supervisor's semantic view of a protected fact. Commitment identity
    /// and causality always come from the original journal record.
    pub fn supervision_report(&self) -> Option<SystemPayload> {
        Some(match self.clone() {
            Self::ReplayLifecycle(event) => SystemPayload::ReplayLifecycle(event),
            Self::SupervisorRegistered { descriptor } => {
                SystemPayload::SupervisorRegistered { descriptor }
            }
            Self::SupervisorCommandDiscarded {
                supervisor,
                terminal_state,
                command,
                disposition,
                error,
            } => SystemPayload::SupervisorCommandDiscarded {
                supervisor,
                terminal_state,
                command,
                disposition,
                error,
            },
            Self::SourceCleanupFailed {
                stage_id,
                stage_name,
                error,
            } => SystemPayload::SourceCleanupFailed {
                stage_id,
                stage_name,
                error,
            },
            Self::ContractStatus {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                pass,
                reader_seq,
                advertised_writer_seq,
                reason,
            } => SystemPayload::ContractStatus {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                pass,
                reader_seq,
                advertised_writer_seq,
                reason,
            },
            Self::ContractResult {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                contract_name,
                status,
                cause,
                reader_seq,
                advertised_writer_seq,
            } => SystemPayload::ContractResult {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                contract_name,
                status,
                cause,
                reader_seq,
                advertised_writer_seq,
            },
            Self::IngressRefusal {
                ingress_key,
                stage_id,
                stage_key,
                reason,
                attempt_seq,
                request_count,
                event_count,
                batch_count,
                http_status,
                retry_after_ms_bucket,
            } => SystemPayload::IngressRefusal {
                ingress_key,
                stage_id,
                stage_key,
                reason,
                attempt_seq,
                request_count,
                event_count,
                batch_count,
                http_status,
                retry_after_ms_bucket,
            },
            Self::StageLifecycle(fact) => {
                let (stage_id, event) = match fact {
                    Fact::Running { stage_id } => (stage_id, Lifecycle::Running),
                    Fact::Draining {
                        stage_id,
                        accounting,
                        ..
                    } => (stage_id, Lifecycle::Draining { accounting }),
                    Fact::Drained { stage_id, .. } => (stage_id, Lifecycle::Drained),
                    Fact::Completed {
                        stage_id,
                        accounting,
                    } => (stage_id, Lifecycle::Completed { accounting }),
                    Fact::Cancelled {
                        stage_id,
                        reason,
                        accounting,
                    } => (stage_id, Lifecycle::Cancelled { reason, accounting }),
                    Fact::Failed {
                        stage_id,
                        error,
                        recoverable,
                        accounting,
                        causal_event_id,
                    } => (
                        stage_id,
                        Lifecycle::Failed {
                            error,
                            recoverable,
                            accounting,
                            causal_event_id,
                        },
                    ),
                };
                SystemPayload::StageLifecycle { stage_id, event }
            }
            _ => return None,
        })
    }
}
