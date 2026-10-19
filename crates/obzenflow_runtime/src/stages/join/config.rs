// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join stage configuration

use crate::stages::common::control_strategies::SignalGate;
use crate::stages::observer::StageObserverBindings;
use obzenflow_core::{StageId, StageKey};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Default Live join fairness cap (FLOWIP-084g).
///
/// When the join is in Live mode, after N reference events without a stream event the supervisor
/// prioritizes polling the stream to prevent starvation.
pub const DEFAULT_REFERENCE_BATCH_CAP: usize = 8;

/// Controls whether the join requires reference EOF before processing the stream.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum JoinReferenceMode {
    /// Current behavior: hydrate the reference side until EOF, then consume stream events.
    #[default]
    FiniteEof,

    /// Live mode: consume reference updates and stream events concurrently (no reference EOF gate).
    Live,

    /// Hydrate the batch catalog, then enrich the batch stream, then run live over
    /// every input. Each phase ends only on the natural EOFs of its witnesses.
    FiniteThenLive,
}

impl JoinReferenceMode {
    /// Whether this mode reaches the `Live` state and records its order there.
    pub fn goes_live(self) -> bool {
        matches!(self, Self::Live | Self::FiniteThenLive)
    }

    /// The `reference_mode` label a join's stage metrics carry.
    pub fn metrics_label(self) -> &'static str {
        match self {
            Self::FiniteEof => "finite_eof",
            Self::Live => "live",
            Self::FiniteThenLive => "finite_then_live",
        }
    }
}

/// Whether a join's catalog keeps the latest value of each key or every version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinCatalogKind {
    /// One value per key; a later row replaces an earlier one.
    LatestValue,
    /// Every version of each key, by the time it took effect.
    Versioned,
}

/// One stage whose natural EOF ends a then-live join's holding state, by its
/// stable key and its stage ID in this run.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhaseWitness {
    pub stage_key: StageKey,
    pub stage_id: StageId,
}

/// The authored natural EOFs that end a then-live join's holding states,
/// computed by the flow build.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhaseWitnesses {
    /// Stages whose natural EOFs, arriving on the catalog, end hydration.
    pub catalog: Vec<PhaseWitness>,
    /// Stages whose natural EOFs, arriving on the stream, end the batch. Empty
    /// when the join has no batch stream input, so hydration leads to `Live`.
    pub batch_stream: Vec<PhaseWitness>,
    /// Whether any stream input has a live source. Without one, the batch's
    /// natural end ends the join.
    pub live_stream: LiveStream,
}

/// Whether any of a then-live join's stream inputs has a live source.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum LiveStream {
    #[default]
    Present,
    Absent,
}

/// Configuration for a join stage
#[derive(Clone, Serialize, Deserialize)]
pub struct JoinConfig {
    /// Stage ID for this join
    pub stage_id: StageId,

    /// Unique name for this join stage
    pub stage_name: String,

    /// Closed observer inputs validated by the concrete join builder.
    #[serde(skip, default)]
    pub(crate) observer_bindings: StageObserverBindings,

    /// Flow name
    pub flow_name: String,

    /// Stage ID for the reference upstream
    pub reference_source_id: StageId,

    /// Stage ID for the stream upstream
    pub stream_source_id: StageId,

    /// Reference join mode (default: FiniteEof).
    pub reference_mode: JoinReferenceMode,

    /// Then-live phase witnesses, `Some` exactly in `FiniteThenLive` mode.
    pub phase_witnesses: Option<PhaseWitnesses>,

    /// Live-mode fairness cap: after N reference events without a stream event, prioritize a
    /// stream poll. `None` disables the cap (pure priority based on selection order).
    ///
    /// Default: `Some(DEFAULT_REFERENCE_BATCH_CAP)`.
    pub reference_batch_cap: Option<usize>,

    /// Control strategy for handling control events
    #[serde(skip)]
    pub control_strategy: Option<Arc<dyn SignalGate>>,

    /// List of upstream stage IDs
    pub upstream_stages: Vec<StageId>,
}

impl JoinConfig {
    pub fn new(
        stage_id: StageId,
        stage_name: impl Into<String>,
        flow_name: impl Into<String>,
        reference_source_id: StageId,
        stream_source_id: StageId,
    ) -> Self {
        Self {
            stage_id,
            stage_name: stage_name.into(),
            observer_bindings: StageObserverBindings::default(),
            flow_name: flow_name.into(),
            reference_source_id,
            stream_source_id,
            reference_mode: JoinReferenceMode::FiniteEof,
            phase_witnesses: None,
            reference_batch_cap: Some(DEFAULT_REFERENCE_BATCH_CAP),
            control_strategy: None,
            upstream_stages: vec![reference_source_id, stream_source_id],
        }
    }

    pub fn with_observer_bindings(mut self, bindings: StageObserverBindings) -> Self {
        self.observer_bindings = bindings;
        self
    }
}
