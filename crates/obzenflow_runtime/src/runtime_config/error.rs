// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Resolution and admission errors, rendered in the startup `config error
//! at <path>: ...` convention so build-side failures read like 010h's.

use obzenflow_core::config::{ConfigAddress, ConfigSource};
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
    /// A scoped entry names a stage the topology does not contain. `known`
    /// lists the real stage names so a typo is obvious.
    UnknownStage {
        key_path: String,
        stage: String,
        known: Vec<String>,
    },
    /// A scoped entry names an edge the topology does not contain. `known`
    /// lists the real edges (`up|>down`).
    UnknownEdge {
        key_path: String,
        upstream: String,
        downstream: String,
        known: Vec<String>,
    },
    /// An exact effect address names no declaration on the configured stage.
    UnknownEffect {
        key_path: String,
        stage: String,
        effect_type: String,
        known: Vec<String>,
    },
    /// The effect exists, but no surviving attachment consumes this key at it.
    UnattachedEffectSubject {
        key_path: String,
        stage: String,
        effect_type: String,
    },
    /// A candidate was admitted at a scope its knob target does not admit,
    /// or a source (CLI/env) was scoped in the first pass.
    ScopeNotAdmitted {
        key_path: String,
        address: ConfigAddress,
        reason: String,
    },
    /// Two candidates with the same (knob, address, source) and different values.
    Conflict {
        key_path: String,
        address: ConfigAddress,
        source: ConfigSource,
    },
    /// A candidate value failed its knob's type or range check.
    InvalidValue {
        key_path: String,
        address: ConfigAddress,
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
            Self::UnknownStage {
                key_path,
                stage,
                known,
            } => {
                write!(
                    f,
                    "config error at {key_path}: configured stage does not exist: '{stage}'"
                )?;
                if let Some(suggestion) = closest_match(stage, known) {
                    write!(f, "; did you mean '{suggestion}'?")?;
                }
                write!(f, " Known stages: {}", join_or_none(known))
            }
            Self::UnknownEdge {
                key_path,
                upstream,
                downstream,
                known,
            } => write!(
                f,
                "config error at {key_path}: configured edge does not exist: {upstream}|>{downstream}. \
                 Known edges: {}",
                join_or_none(known)
            ),
            Self::UnknownEffect {
                key_path,
                stage,
                effect_type,
                known,
            } => write!(
                f,
                "config error at {key_path}: effect '{effect_type}' is not declared by stage \
                 '{stage}'. Known effects: {}",
                join_or_none(known)
            ),
            Self::UnattachedEffectSubject {
                key_path,
                stage,
                effect_type,
            } => write!(
                f,
                "config error at {key_path}: effect '{effect_type}' is declared by stage \
                 '{stage}', but no surviving attachment consumes this key"
            ),
            Self::ScopeNotAdmitted {
                key_path,
                address,
                reason,
            } => write!(
                f,
                "config error at {key_path}: entry at {address} is not admitted: {reason}"
            ),
            Self::Conflict {
                key_path,
                address,
                source,
            } => write!(
                f,
                "config error at {key_path}: conflicting {source} candidates at {address}"
            ),
            Self::InvalidValue {
                key_path,
                address,
                message,
            } => write!(f, "config error at {key_path} ({address}): {message}"),
            Self::UnknownKnob { key_path } => {
                write!(f, "config error at {key_path}: unknown configuration key")
            }
        }
    }
}

impl std::error::Error for ConfigResolveError {}

/// Render a known-name list, or `(none)` when the topology has no candidates.
fn join_or_none(names: &[String]) -> String {
    if names.is_empty() {
        "(none)".to_string()
    } else {
        names.join(", ")
    }
}

/// The closest known name to a typo, when one is within a small edit distance,
/// so `web_order` suggests `web_orders`. `None` when nothing is close enough.
fn closest_match<'a>(target: &str, candidates: &'a [String]) -> Option<&'a str> {
    let budget = 2.max(target.len() / 3);
    candidates
        .iter()
        .map(|candidate| (levenshtein(target, candidate), candidate.as_str()))
        .filter(|(distance, _)| *distance <= budget)
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, candidate)| candidate)
}

/// Edit distance between two names (insert/delete/substitute), for typo hints.
fn levenshtein(a: &str, b: &str) -> usize {
    let b_chars: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b_chars.len()).collect();
    let mut curr = vec![0usize; b_chars.len() + 1];
    for (i, a_char) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, &b_char) in b_chars.iter().enumerate() {
            let cost = usize::from(a_char != b_char);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b_chars.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_stage_lists_known_and_suggests_the_closest() {
        let err = ConfigResolveError::UnknownStage {
            key_path: "runtime.backpressure.mode".to_string(),
            stage: "web_order".to_string(),
            known: vec!["store_orders".to_string(), "web_orders".to_string()],
        };
        let message = err.to_string();
        assert!(message.contains("does not exist: 'web_order'"), "{message}");
        assert!(message.contains("did you mean 'web_orders'?"), "{message}");
        assert!(
            message.contains("Known stages: store_orders, web_orders"),
            "{message}"
        );
    }

    #[test]
    fn unknown_stage_omits_the_hint_when_nothing_is_close() {
        let err = ConfigResolveError::UnknownStage {
            key_path: "k".to_string(),
            stage: "totally_different".to_string(),
            known: vec!["a".to_string()],
        };
        let message = err.to_string();
        assert!(!message.contains("did you mean"), "{message}");
        assert!(message.contains("Known stages: a"), "{message}");
    }
}
