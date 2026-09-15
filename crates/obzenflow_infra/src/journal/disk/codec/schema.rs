// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Format-3 positions are explicit protocol constants, not Rust field order.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(super) enum DefinitionKind {
    Writer = 0,
    Context = 1,
    Origin = 2,
    Descriptor = 3,
    CaptureScope = 4,
    ClockWriter = 5,
}

impl DefinitionKind {
    pub(super) fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::Writer,
            1 => Self::Context,
            2 => Self::Origin,
            3 => Self::Descriptor,
            4 => Self::CaptureScope,
            5 => Self::ClockWriter,
            _ => return None,
        })
    }

    pub(super) fn body(self) -> Kind {
        match self {
            Self::Writer => Kind::Struct(Shape::Writer),
            Self::Context => Kind::Struct(Shape::Context),
            Self::Origin => Kind::Struct(Shape::Origin),
            Self::Descriptor => Kind::Text,
            Self::CaptureScope => Kind::Struct(Shape::CaptureScope),
            Self::ClockWriter => Kind::ClockKey,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Kind {
    Unsigned,
    Float,
    Boolean,
    Text,
    Id,
    FlowId,
    ClockKey,
    Timestamp,
    PacketCapture,
    SnapshotCapture,
    Json,
    /// Complete binary values for typed, non-hot-path framework structures.
    Value,
    Enum(&'static [&'static str]),
    Struct(Shape),
    List(&'static Kind),
    Clock,
    Definition(DefinitionKind),
}

#[derive(Debug, Clone, Copy)]
pub(super) enum DefaultValue {
    Zero,
    FloatZero,
    False,
    EmptyList,
    Text(&'static str),
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Field {
    pub(super) name: &'static str,
    pub(super) kind: Kind,
    pub(super) default: Option<DefaultValue>,
}

const fn field(name: &'static str, kind: Kind) -> Field {
    let default = match kind {
        Kind::Unsigned => Some(DefaultValue::Zero),
        Kind::Float => Some(DefaultValue::FloatZero),
        Kind::Boolean => Some(DefaultValue::False),
        Kind::List(_) => Some(DefaultValue::EmptyList),
        _ => None,
    };
    Field {
        name,
        kind,
        default,
    }
}

const fn text_default(name: &'static str, value: &'static str) -> Field {
    Field {
        name,
        kind: Kind::Text,
        default: Some(DefaultValue::Text(value)),
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Shape {
    Provenance,
    Event,
    Journal,
    Writer,
    Context,
    Processing,
    Causality,
    Correlation,
    Origin,
    Runtime,
    Accounting,
    OutputCount,
    InputCount,
    Observation,
    Capture,
    CaptureScope,
    RuntimeSnapshot,
    Progress,
    Measurements,
    Timing,
    Window,
    CircuitBreaker,
    RateLimiter,
    EffectCircuitBreaker,
    EffectRateLimiter,
    Metrics,
    Sli,
    GroupMember,
}

const EVENT_KINDS: &[&str] = &[
    "fact",
    "flow_signal",
    "delivery",
    "execution",
    "composite_data",
    "system",
];
const WRITER_KINDS: &[&str] = &["Stage", "System"];
const CAPTURE_REASONS: &[&str] = &["record", "initial", "periodic", "final"];
const CIRCUIT_STATES: &[&str] = &["closed", "open", "half_open"];

impl Shape {
    pub(super) fn fields(self) -> &'static [Field] {
        use DefinitionKind as D;
        use Kind as K;
        use Shape as S;
        match self {
            S::Provenance => {
                const {
                    &[
                        field("event", K::Struct(S::Event)),
                        field("journal", K::Struct(S::Journal)),
                    ]
                }
            }
            S::Event => {
                const {
                    &[
                        field("id", K::Id),
                        field("writer_id", K::Definition(D::Writer)),
                        field("event_kind", K::Enum(EVENT_KINDS)),
                        field("event_type", K::Definition(D::Descriptor)),
                        field("causality", K::Struct(S::Causality)),
                        field("flow_context", K::Definition(D::Context)),
                        field("processing", K::Struct(S::Processing)),
                        field("intent", K::Value),
                        field("correlation", K::Struct(S::Correlation)),
                        field("replay_context", K::Value),
                        field("ingress_context", K::Value),
                        field("cycle_depth", K::Unsigned),
                        field("cycle_scc_id", K::Value),
                        field("effect_provenance", K::Value),
                        field("admission_seq", K::Unsigned),
                        field("runtime", K::Struct(S::Runtime)),
                        field("composite_activations", K::List(&K::Value)),
                        field("timestamp", K::Unsigned),
                    ]
                }
            }
            S::Journal => {
                const {
                    &[
                        field("journal_writer_id", K::Id),
                        field("vector_clock", K::Clock),
                        field("timestamp", K::Timestamp),
                        field("journal_group_id", K::Text),
                        field("journal_group_member", K::Struct(S::GroupMember)),
                    ]
                }
            }
            S::Writer => const { &[field("type", K::Enum(WRITER_KINDS)), field("id", K::Id)] },
            S::Context => {
                const {
                    &[
                        field("flow_name", K::Text),
                        field("flow_id", K::Text),
                        field("stage_name", K::Text),
                        field("stage_id", K::Id),
                        field("stage_type", K::Text),
                    ]
                }
            }
            S::Processing => {
                const {
                    &[
                        text_default("processed_by", "unknown"),
                        field("event_time", K::Unsigned),
                        Field {
                            name: "status",
                            kind: K::Value,
                            default: Some(DefaultValue::Text("Success")),
                        },
                        field("error_hops_remaining", K::Unsigned),
                    ]
                }
            }
            S::Causality => const { &[field("parent_ids", K::List(&K::Id))] },
            S::Correlation => {
                const {
                    &[
                        field("ids", K::List(&K::Id)),
                        field("truncated", K::Boolean),
                        field("payload", K::Definition(D::Origin)),
                    ]
                }
            }
            S::Origin => {
                const {
                    &[
                        field("entry_time_ns", K::Unsigned),
                        field("entry_stage", K::Text),
                        field("entry_event_id", K::Id),
                        field("metadata", K::Json),
                    ]
                }
            }
            S::Runtime => const { &[field("accounting", K::Struct(S::Accounting))] },
            S::Accounting => {
                const {
                    &[
                        field("events_processed_total", K::Unsigned),
                        field("events_accumulated_total", K::Unsigned),
                        field("events_emitted_total", K::Unsigned),
                        field("terminal_groups_committed_total", K::Unsigned),
                        field("terminal_group_commit_failures_total", K::Unsigned),
                        field("errors_total", K::Unsigned),
                        field("failures_total", K::Unsigned),
                        field("errors_by_kind", K::Value),
                        field(
                            "data_outputs_by_event_type",
                            K::List(&K::Struct(S::OutputCount)),
                        ),
                        field(
                            "data_inputs_by_upstream_event_type",
                            K::List(&K::Struct(S::InputCount)),
                        ),
                    ]
                }
            }
            S::OutputCount => {
                const {
                    &[
                        field("event_type", K::Definition(D::Descriptor)),
                        field("total", K::Unsigned),
                    ]
                }
            }
            S::InputCount => {
                const {
                    &[
                        field("upstream", K::Id),
                        field("event_type", K::Definition(D::Descriptor)),
                        field("total", K::Unsigned),
                    ]
                }
            }
            S::Observation => {
                const {
                    &[
                        field("capture", K::PacketCapture),
                        field("runtime", K::Struct(S::Measurements)),
                        field("runtime_snapshot", K::Struct(S::RuntimeSnapshot)),
                        field("processing_time", K::Unsigned),
                        field("metrics", K::Struct(S::Metrics)),
                        field("sli", K::Struct(S::Sli)),
                        field("records", K::List(&K::Value)),
                    ]
                }
            }
            S::Capture => {
                const {
                    &[
                        field("capture_scope", K::Definition(D::CaptureScope)),
                        field("observer", K::Definition(D::Writer)),
                        field("capture_seq", K::Unsigned),
                        field("capture_reason", K::Enum(CAPTURE_REASONS)),
                        field("observed_at_ms", K::Unsigned),
                    ]
                }
            }
            S::CaptureScope => {
                const {
                    &[
                        field("flow_id", K::FlowId),
                        field("resume_generation", K::Unsigned),
                    ]
                }
            }
            S::RuntimeSnapshot => {
                const {
                    &[
                        field("capture", K::SnapshotCapture),
                        field("progress", K::Struct(S::Progress)),
                        field("fsm_state", K::Definition(D::Descriptor)),
                    ]
                }
            }
            S::Progress => {
                const {
                    &[
                        field("reader_seq", K::Unsigned),
                        field("receipted_seq", K::Unsigned),
                        field("writer_seq", K::Unsigned),
                        field("last_consumed_event_id", K::Id),
                        field("last_consumed_writer", K::Id),
                        field("last_consumed_vector_clock", K::Clock),
                        field("last_receipted_event_id", K::Id),
                        field("last_receipted_vector_clock", K::Clock),
                        field("last_emitted_event_id", K::Id),
                        field("last_emitted_writer", K::Definition(D::Writer)),
                    ]
                }
            }
            S::Measurements => {
                const {
                    &[
                        field("in_flight", K::Unsigned),
                        field("join_reference_since_last_stream", K::Unsigned),
                        field("time_in_state_ms", K::Unsigned),
                        field("event_loops_total", K::Unsigned),
                        field("event_loops_with_work_total", K::Unsigned),
                        field("timing", K::Struct(S::Timing)),
                        field("circuit_breaker", K::Struct(S::CircuitBreaker)),
                        field("rate_limiter", K::Struct(S::RateLimiter)),
                        field(
                            "effect_circuit_breakers",
                            K::List(&K::Struct(S::EffectCircuitBreaker)),
                        ),
                        field(
                            "effect_rate_limiters",
                            K::List(&K::Struct(S::EffectRateLimiter)),
                        ),
                    ]
                }
            }
            S::Timing => {
                const {
                    &[
                        field("processing_time_count", K::Unsigned),
                        field("processing_time_sum_nanos", K::Unsigned),
                        field("recent_p50_ms", K::Unsigned),
                        field("recent_p90_ms", K::Unsigned),
                        field("recent_p95_ms", K::Unsigned),
                        field("recent_p99_ms", K::Unsigned),
                        field("recent_p999_ms", K::Unsigned),
                        field("window", K::Struct(S::Window)),
                    ]
                }
            }
            S::Window => {
                const {
                    &[
                        field("started_at_ms", K::Unsigned),
                        field("ended_at_ms", K::Unsigned),
                    ]
                }
            }
            S::CircuitBreaker => {
                const {
                    &[
                        field("observed_state", K::Enum(CIRCUIT_STATES)),
                        field("requests_total", K::Unsigned),
                        field("successes_total", K::Unsigned),
                        field("failures_total", K::Unsigned),
                        field("slow_total", K::Unsigned),
                        field("rejections_total", K::Unsigned),
                        field("opened_total", K::Unsigned),
                        field("time_closed_seconds", K::Float),
                        field("time_open_seconds", K::Float),
                        field("time_half_open_seconds", K::Float),
                    ]
                }
            }
            S::RateLimiter => {
                const {
                    &[
                        field("events_total", K::Unsigned),
                        field("delayed_total", K::Unsigned),
                        field("tokens_consumed_total", K::Float),
                        field("delay_seconds_total", K::Float),
                        field("bucket_tokens", K::Float),
                        field("bucket_capacity", K::Float),
                    ]
                }
            }
            S::EffectCircuitBreaker => {
                const {
                    &[
                        field("effect_type", K::Definition(D::Descriptor)),
                        field("cb_requests_total", K::Unsigned),
                        field("cb_successes_total", K::Unsigned),
                        field("cb_failures_total", K::Unsigned),
                        field("cb_slow_total", K::Unsigned),
                        field("cb_rejections_total", K::Unsigned),
                        field("cb_opened_total", K::Unsigned),
                        field("cb_time_closed_seconds", K::Float),
                        field("cb_time_open_seconds", K::Float),
                        field("cb_time_half_open_seconds", K::Float),
                        field("cb_state", K::Float),
                    ]
                }
            }
            S::EffectRateLimiter => {
                const {
                    &[
                        field("effect_type", K::Definition(D::Descriptor)),
                        field("rl_events_total", K::Unsigned),
                        field("rl_delayed_total", K::Unsigned),
                        field("rl_tokens_consumed_total", K::Float),
                        field("rl_delay_seconds_total", K::Float),
                        field("rl_bucket_tokens", K::Float),
                        field("rl_bucket_capacity", K::Float),
                    ]
                }
            }
            S::Metrics => {
                const {
                    &[
                        field("events_processed", K::Unsigned),
                        field("events_in_flight", K::Unsigned),
                        field("queue_depth", K::Unsigned),
                        field("processing_rate", K::Float),
                        field("error_rate", K::Float),
                        field("latency_p50_ms", K::Float),
                        field("latency_p99_ms", K::Float),
                    ]
                }
            }
            S::Sli => {
                const {
                    &[
                        field("availability", K::Float),
                        field("error_budget_remaining", K::Float),
                        field("latency_budget_used", K::Float),
                    ]
                }
            }
            S::GroupMember => const { &[field("index", K::Unsigned), field("size", K::Unsigned)] },
        }
    }
}

/// Fixed tokens for nested typed framework fields not covered by a hot-path
/// positional structure. Unknown dynamic map keys retain their UTF-8 spelling.
/// Opaque application/custom JSON does not pass through this table.
pub(super) const NAMES: &[&str] = &[
    "observation_type",
    "metadata",
    "effect_type",
    "window_duration_s",
    "requests_processed",
    "requests_rejected",
    "observed_state",
    "consecutive_failures",
    "rejection_rate",
    "successes_total",
    "failures_total",
    "opened_total",
    "time_in_closed_seconds",
    "time_in_open_seconds",
    "time_in_half_open_seconds",
    "window_ms",
    "delayed_events",
    "delay_ms_total",
    "delay_ms_max",
    "limit_rate",
    "utilization_percent",
    "events_in_window",
    "window_size_ms",
    "min_credit",
    "limiting_downstream_stage_id",
    "cpu_percent",
    "memory_bytes",
    "thread_count",
    "requests_total",
    "responses_2xx",
    "responses_4xx",
    "responses_5xx",
    "rate_limited_total",
    "retries_total",
    "events_decoded_total",
    "wait_seconds_rate_limit",
    "wait_seconds_poll_interval",
    "wait_seconds_backoff",
    "rerender_attempts_total",
    "max_decomposition_depth_reached",
    "budget_overhead_tokens",
    "excluded_items",
    "activity",
    "handler_blocked_ms",
    "last_consumed_event_id",
    "last_output_event_id",
    "upstream",
    "reader",
    "state",
    "idle_ms",
    "last_reader_seq",
    "last_event_id",
    "snapshot",
    "routes",
    "surface_name",
    "method",
    "path",
    "status_class",
    "request_duration_ms_total",
    "request_bytes_total",
    "response_bytes_total",
    "schema_version",
    "provider",
    "model",
    "hashes",
    "usage",
    "estimated_input_tokens",
    "estimated_input_resolution",
    "cache",
    "version",
    "prompt_hash",
    "params_hash",
    "schema_hash",
    "mode",
    "hit",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "prompt_tokens",
    "completion_tokens",
    "cached_input_tokens",
    "reasoning_tokens",
    "message",
    "kind",
    "Error",
    "Command",
    "Query",
    "Event",
    "Document",
    "action",
    "target",
    "question",
    "fact",
    "content",
    "id",
    "entry_time_ns",
    "entry_event_id",
    "composite_id",
    "invocation_id",
    "parent_ids",
    "stage_id",
    "flow_id",
    "type",
];
