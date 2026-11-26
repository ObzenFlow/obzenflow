// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Invocation-terminal, non-disclosing binding authority faults (FLOWIP-132a).

use crate::stages::common::handler_error::StageFatal;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use std::fmt;

/// Closed reason for a declaration/invocation binding disagreement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingMismatchKind {
    Mode,
    ConstructionFamily,
    Evidence,
}

/// Closed, curated failure set for live effect authority.
///
/// Every value carried here is a validated public identifier. Evidence,
/// targets, resolver causes, ports, and family tokens are deliberately absent.
#[derive(Clone, PartialEq, Eq)]
pub struct BindingAuthorityFault {
    kind: BindingAuthorityFaultKind,
}

#[derive(Clone, PartialEq, Eq)]
enum BindingAuthorityFaultKind {
    RegistrationMissing {
        effect_type: &'static str,
        stage: String,
    },
    ResolutionFailed {
        effect_type: &'static str,
        stage: String,
        slot: &'static str,
    },
    BindingMismatch {
        effect_type: &'static str,
        stage: String,
        kind: BindingMismatchKind,
    },
    TargetInvariantViolation {
        effect_type: &'static str,
        stage: String,
        slot: &'static str,
        recorded: bool,
    },
}

impl BindingAuthorityFault {
    pub(crate) fn registration_missing(effect_type: &'static str, stage: &str) -> Self {
        Self {
            kind: BindingAuthorityFaultKind::RegistrationMissing {
                effect_type: curated_effect_type(effect_type),
                stage: stage.to_owned(),
            },
        }
    }

    pub(crate) fn resolution_failed(
        effect_type: &'static str,
        stage: &str,
        slot: &'static str,
    ) -> Self {
        Self {
            kind: BindingAuthorityFaultKind::ResolutionFailed {
                effect_type: curated_effect_type(effect_type),
                stage: stage.to_owned(),
                slot,
            },
        }
    }

    pub(crate) fn binding_mismatch(
        effect_type: &'static str,
        stage: &str,
        kind: BindingMismatchKind,
    ) -> Self {
        Self {
            kind: BindingAuthorityFaultKind::BindingMismatch {
                effect_type: curated_effect_type(effect_type),
                stage: stage.to_owned(),
                kind,
            },
        }
    }

    pub(crate) fn target_invariant_violation(
        effect_type: &'static str,
        stage: &str,
        slot: &'static str,
        recorded: bool,
    ) -> Self {
        Self {
            kind: BindingAuthorityFaultKind::TargetInvariantViolation {
                effect_type: curated_effect_type(effect_type),
                stage: stage.to_owned(),
                slot,
                recorded,
            },
        }
    }

    pub(crate) fn stage_fatal(&self) -> StageFatal {
        StageFatal::new(
            StageFatalCode::Configuration,
            self.reason(),
            self.to_string(),
        )
    }

    pub fn reason(&self) -> StageFatalReason {
        match &self.kind {
            BindingAuthorityFaultKind::RegistrationMissing { .. } => {
                StageFatalReason::EffectPortRegistrationMissing
            }
            BindingAuthorityFaultKind::ResolutionFailed { .. } => {
                StageFatalReason::EffectPortResolutionFailed
            }
            BindingAuthorityFaultKind::BindingMismatch { .. } => {
                StageFatalReason::EffectPortBindingMismatch
            }
            BindingAuthorityFaultKind::TargetInvariantViolation { .. } => {
                StageFatalReason::EffectPortTargetInvariantViolation
            }
        }
    }

    pub fn mismatch_kind(&self) -> Option<BindingMismatchKind> {
        match &self.kind {
            BindingAuthorityFaultKind::BindingMismatch { kind, .. } => Some(*kind),
            _ => None,
        }
    }
}

fn curated_effect_type(effect_type: &'static str) -> &'static str {
    if super::binding::validate_effect_type(effect_type).is_ok() {
        effect_type
    } else {
        // Invalid author input must never be reflected through a framework
        // diagnostic, even if an internal invariant accidentally bypasses
        // descriptor materialisation.
        "invalid_effect_type"
    }
}

impl fmt::Display for BindingAuthorityFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            BindingAuthorityFaultKind::RegistrationMissing { effect_type, stage } => write!(
                formatter,
                "effect '{effect_type}' of stage '{stage}' has no installed registration"
            ),
            BindingAuthorityFaultKind::ResolutionFailed {
                effect_type,
                stage,
                slot,
            } => write!(
                formatter,
                "effect '{effect_type}' of stage '{stage}' failed to resolve slot '{slot}'"
            ),
            BindingAuthorityFaultKind::BindingMismatch {
                effect_type,
                stage,
                kind,
            } => {
                let reason = match kind {
                    BindingMismatchKind::Mode => "mode",
                    BindingMismatchKind::ConstructionFamily => "construction family",
                    BindingMismatchKind::Evidence => "evidence",
                };
                write!(
                    formatter,
                    "performed effect '{effect_type}' of stage '{stage}' disagrees with its declaration ({reason})"
                )
            }
            BindingAuthorityFaultKind::TargetInvariantViolation {
                effect_type,
                stage,
                slot,
                recorded,
            } => {
                let source = if *recorded { "recorded" } else { "live" };
                write!(
                    formatter,
                    "{source} target invariant failed for effect '{effect_type}' of stage '{stage}', slot '{slot}'"
                )
            }
        }
    }
}

impl fmt::Debug for BindingAuthorityFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

impl std::error::Error for BindingAuthorityFault {}

#[cfg(test)]
mod tests {
    use super::{BindingAuthorityFault, BindingMismatchKind};

    #[test]
    fn fault_display_names_effect_stage() {
        let effect = "hn.digest.publish";
        let stage = "digest_summary";
        let messages = [
            BindingAuthorityFault::registration_missing(effect, stage),
            BindingAuthorityFault::resolution_failed(effect, stage, "port"),
            BindingAuthorityFault::binding_mismatch(effect, stage, BindingMismatchKind::Evidence),
            BindingAuthorityFault::target_invariant_violation(effect, stage, "port", true),
        ]
        .map(|fault| fault.to_string());
        assert_eq!(
            messages,
            [
                "effect 'hn.digest.publish' of stage 'digest_summary' has no installed registration",
                "effect 'hn.digest.publish' of stage 'digest_summary' failed to resolve slot 'port'",
                "performed effect 'hn.digest.publish' of stage 'digest_summary' disagrees with its \
                 declaration (evidence)",
                "recorded target invariant failed for effect 'hn.digest.publish' of stage \
                 'digest_summary', slot 'port'",
            ]
        );
    }
}
