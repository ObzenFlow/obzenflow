// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replayable identity for one fact admitted through a composite input port.

use crate::id::CompositeId;
use crate::EventId;
use serde::{Deserialize, Serialize};

/// The entry side of an exact composite boundary-duration observation.
///
/// This is integration metadata, not a second domain occurrence. Every field
/// is reconstructable from the durable topology binding and admitted entry
/// fact. Derived events carry it so an output boundary fact can be paired with
/// the exact entry fact under fan-out and fan-in.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompositeActivationContext {
    pub composite_id: CompositeId,
    pub activation: EventId,
    pub entry_port: String,
    /// Persisted `processing_info.event_time` of the entry fact, in Unix ms.
    pub entered_at_ms: u64,
}

impl CompositeActivationContext {
    pub fn new(
        composite_id: CompositeId,
        activation: EventId,
        entry_port: impl Into<String>,
        entered_at_ms: u64,
    ) -> Self {
        Self {
            composite_id,
            activation,
            entry_port: entry_port.into(),
            entered_at_ms,
        }
    }
}

/// Conflicting durable entry evidence for one composite activation identity.
///
/// Activation identity is `(composite_id, activation, entry_port)`. Its
/// persisted entry timestamp is therefore single-valued. Treating two
/// timestamps for that identity as an ordinary duplicate would make union
/// order decide the reported boundary duration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[doc(hidden)]
#[error(
    "composite activation '{composite_id}' event {activation} port '{entry_port}' has conflicting entry timestamps {first_entered_at_ms} and {second_entered_at_ms}"
)]
pub struct CompositeActivationConflict {
    composite_id: CompositeId,
    activation: EventId,
    entry_port: String,
    first_entered_at_ms: u64,
    second_entered_at_ms: u64,
}

impl CompositeActivationConflict {
    fn new(first: &CompositeActivationContext, second: &CompositeActivationContext) -> Self {
        Self {
            composite_id: first.composite_id.clone(),
            activation: first.activation,
            entry_port: first.entry_port.clone(),
            first_entered_at_ms: first.entered_at_ms,
            second_entered_at_ms: second.entered_at_ms,
        }
    }
}

fn same_identity(left: &CompositeActivationContext, right: &CompositeActivationContext) -> bool {
    left.composite_id == right.composite_id
        && left.activation == right.activation
        && left.entry_port == right.entry_port
}

/// Return the canonical set union of two activation-provenance collections.
///
/// Exact duplicates collapse idempotently. Results are sorted by durable
/// activation identity, independent of parent arrival order. Conflicting
/// timestamps fail instead of silently selecting the first value observed.
#[doc(hidden)]
pub fn union_composite_activations(
    left: &[CompositeActivationContext],
    right: &[CompositeActivationContext],
) -> Result<Vec<CompositeActivationContext>, CompositeActivationConflict> {
    let mut candidates: Vec<_> = left.iter().chain(right).cloned().collect();
    candidates.sort_by(|left, right| {
        (
            left.composite_id.as_ref(),
            left.activation,
            left.entry_port.as_str(),
            left.entered_at_ms,
        )
            .cmp(&(
                right.composite_id.as_ref(),
                right.activation,
                right.entry_port.as_str(),
                right.entered_at_ms,
            ))
    });

    let mut canonical: Vec<CompositeActivationContext> = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if let Some(previous) = canonical.last() {
            if same_identity(previous, &candidate) {
                if previous.entered_at_ms != candidate.entered_at_ms {
                    return Err(CompositeActivationConflict::new(previous, &candidate));
                }
                continue;
            }
        }
        canonical.push(candidate);
    }

    Ok(canonical)
}

#[cfg(test)]
mod union_tests {
    use super::*;

    fn activation(
        composite: &str,
        activation: u128,
        port: &str,
        entered_at_ms: u64,
    ) -> CompositeActivationContext {
        CompositeActivationContext::new(
            CompositeId::new(composite),
            EventId::from(ulid::Ulid::from(activation)),
            port,
            entered_at_ms,
        )
    }

    #[test]
    fn union_is_idempotent_and_independent_of_parent_order() {
        let first = activation("checkout", 2, "in", 20);
        let second = activation("catalog", 1, "lookup", 10);

        let left_then_right = union_composite_activations(
            std::slice::from_ref(&first),
            std::slice::from_ref(&second),
        )
        .unwrap();
        let right_then_left = union_composite_activations(
            std::slice::from_ref(&second),
            std::slice::from_ref(&first),
        )
        .unwrap();
        let duplicate = union_composite_activations(&left_then_right, &right_then_left).unwrap();

        assert_eq!(left_then_right, right_then_left);
        assert_eq!(duplicate, left_then_right);
        assert_eq!(left_then_right, vec![second, first]);
    }

    #[test]
    fn union_rejects_conflicting_timestamps_for_one_identity() {
        let first = activation("checkout", 1, "in", 10);
        let conflicting = activation("checkout", 1, "in", 11);

        let error = union_composite_activations(&[first], &[conflicting]).unwrap_err();

        assert!(error
            .to_string()
            .contains("conflicting entry timestamps 10 and 11"));
    }
}
