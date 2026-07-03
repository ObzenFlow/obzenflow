// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Resolution and admission errors, rendered in the startup `config error
//! at <path>: ...` convention so build-side failures read like 010h's.

use obzenflow_core::config::{ConfigScope, ConfigSource};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigResolveError {
    /// A required knob resolved nothing at any admissible scope
    /// (FLOWIP-010 §2 required knobs, §4c edge naming).
    RequiredKnobUnresolved {
        key_path: String,
        /// `"edge enricher|>merger"`, `"stage 'enricher'"`, `"flow"`, `"the runtime"`.
        point: String,
        /// Pre-rendered "supply it at one of: ..." help from the ladder.
        supply_help: String,
    },
    /// A scoped entry names a stage the topology does not contain.
    UnknownStage { key_path: String, stage: String },
    /// A scoped entry names an edge the topology does not contain.
    UnknownEdge {
        key_path: String,
        upstream: String,
        downstream: String,
    },
    /// A candidate was admitted at a scope its knob target does not admit,
    /// or a source (CLI/env) was scoped in the first pass.
    ScopeNotAdmitted {
        key_path: String,
        scope: ConfigScope,
        reason: String,
    },
    /// Two candidates with the same (knob, scope, source) and different values.
    Conflict {
        key_path: String,
        scope: ConfigScope,
        source: ConfigSource,
    },
    /// A candidate value failed its knob's type or range check.
    InvalidValue {
        key_path: String,
        scope: ConfigScope,
        message: String,
    },
    /// A key path with no registry entry reached the resolver.
    UnknownKnob { key_path: String },
}

impl fmt::Display for ConfigResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequiredKnobUnresolved {
                key_path,
                point,
                supply_help,
            } => write!(
                f,
                "config error at {key_path}: required knob unresolved for {point}; {supply_help}"
            ),
            Self::UnknownStage { key_path, stage } => write!(
                f,
                "config error at {key_path}: configured stage does not exist: '{stage}'"
            ),
            Self::UnknownEdge {
                key_path,
                upstream,
                downstream,
            } => write!(
                f,
                "config error at {key_path}: configured edge does not exist: {upstream}|>{downstream}"
            ),
            Self::ScopeNotAdmitted {
                key_path,
                scope,
                reason,
            } => write!(
                f,
                "config error at {key_path}: entry at {scope} scope is not admitted: {reason}"
            ),
            Self::Conflict {
                key_path,
                scope,
                source,
            } => write!(
                f,
                "config error at {key_path}: conflicting {source} candidates at {scope} scope"
            ),
            Self::InvalidValue {
                key_path,
                scope,
                message,
            } => write!(f, "config error at {key_path} ({scope} scope): {message}"),
            Self::UnknownKnob { key_path } => {
                write!(f, "config error at {key_path}: unknown configuration key")
            }
        }
    }
}

impl std::error::Error for ConfigResolveError {}
