// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Per-edge backpressure decisions fed into the flow's backpressure plan.
//!
//! Configuration alone sets backpressure, through the scope ladder from the
//! flow down to a stage's edges.

use obzenflow_core::config::ConfigSource;
use obzenflow_runtime::runtime_config::BackpressureMode;

/// What a resolved edge contributes to the `BackpressurePlan`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EdgeBackpressure {
    /// Enforce with the resolved window and stall timeout.
    Enforce,
    /// Track in-flight accounting without blocking.
    Track,
    /// No backpressure state for this edge.
    Disabled,
}

/// The per-edge plan-feeding decision, provenance-aware for the cycle
/// auto-enable: a cycle-internal edge whose `off` is only the built-in default
/// upgrades to `track`, but an explicit `off` from configuration is respected.
pub(crate) fn resolve_edge_backpressure(
    mode: BackpressureMode,
    mode_source: ConfigSource,
    cycle_internal: bool,
) -> EdgeBackpressure {
    match mode {
        BackpressureMode::Enforce => EdgeBackpressure::Enforce,
        BackpressureMode::Track => EdgeBackpressure::Track,
        BackpressureMode::Off => {
            if cycle_internal && matches!(mode_source, ConfigSource::Default) {
                EdgeBackpressure::Track
            } else {
                EdgeBackpressure::Disabled
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_off_cycle_internal_edge_upgrades_to_track() {
        assert_eq!(
            resolve_edge_backpressure(BackpressureMode::Off, ConfigSource::Default, true),
            EdgeBackpressure::Track
        );
    }

    #[test]
    fn explicit_off_survives_auto_enable_on_a_cycle_edge() {
        for source in [ConfigSource::Dsl, ConfigSource::File, ConfigSource::Env] {
            assert_eq!(
                resolve_edge_backpressure(BackpressureMode::Off, source.clone(), true),
                EdgeBackpressure::Disabled,
                "explicit off from {source:?} must survive the auto-enable"
            );
        }
    }

    #[test]
    fn default_off_acyclic_edge_stays_disabled() {
        assert_eq!(
            resolve_edge_backpressure(BackpressureMode::Off, ConfigSource::Default, false),
            EdgeBackpressure::Disabled
        );
    }

    #[test]
    fn enforce_and_track_pass_through_regardless_of_cycle_or_source() {
        for cycle_internal in [true, false] {
            assert_eq!(
                resolve_edge_backpressure(
                    BackpressureMode::Enforce,
                    ConfigSource::Default,
                    cycle_internal
                ),
                EdgeBackpressure::Enforce
            );
            assert_eq!(
                resolve_edge_backpressure(
                    BackpressureMode::Track,
                    ConfigSource::File,
                    cycle_internal
                ),
                EdgeBackpressure::Track
            );
        }
    }
}
