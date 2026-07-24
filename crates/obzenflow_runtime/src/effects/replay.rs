// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
pub(super) fn effect_record_from_event(
    event: &ChainEvent,
) -> Result<Option<EffectRecord>, EffectError> {
    match &event.content {
        ChainEventContent::Data { event_type, .. }
            if EffectAttemptStarted::event_type_matches(event_type)
                || EffectRecoveryAbandoned::event_type_matches(event_type) =>
        {
            // These framework rows participate in attempt-history folding,
            // not in the legacy EffectRecord outcome group.
            Ok(None)
        }
        ChainEventContent::Data {
            event_type,
            payload,
        } if is_framework_effect_event_type(event_type) => {
            let provenance = event.effect_provenance.as_ref().ok_or_else(|| {
                EffectError::EffectProvenanceMismatch(format!(
                    "reserved framework effect event `{event_type}` is missing effect_provenance"
                ))
            })?;
            if !provenance.fact_owner.is_framework() {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "reserved framework effect event `{event_type}` is not marked as framework-owned"
                )));
            }

            let record: EffectRecord = serde_json::from_value(payload.clone())
                .map_err(|e| EffectError::Serialization(e.to_string()))?;
            validate_effect_record_provenance(event_type, &record, provenance)?;
            Ok(Some(record))
        }
        ChainEventContent::Data {
            event_type,
            payload,
        } => {
            let Some(provenance) = event.effect_provenance.as_ref() else {
                return Ok(None);
            };
            if provenance.fact_owner.is_framework() {
                return Err(EffectError::EffectProvenanceMismatch(
                    "framework-owned effect provenance must use a reserved framework effect event type"
                        .to_string(),
                ));
            }
            let outcome_fact_ordinal = provenance.outcome_fact_ordinal.ok_or_else(|| {
                EffectError::EffectProvenanceMismatch(
                    "domain effect outcome facts must set outcome_fact_ordinal".to_string(),
                )
            })?;
            let outcome_fact_count = provenance.outcome_fact_count.ok_or_else(|| {
                EffectError::EffectProvenanceMismatch(
                    "domain effect outcome facts must set outcome_fact_count".to_string(),
                )
            })?;

            let record = EffectRecord {
                cursor: provenance.cursor.clone(),
                descriptor_hash: provenance.descriptor_hash.clone(),
                descriptor: provenance.descriptor.clone(),
                outcome: EffectOutcomePayload::SucceededFact {
                    event_type: event_type.clone().into(),
                    output: payload.clone(),
                    outcome_fact_ordinal,
                    outcome_fact_count,
                },
                origin: provenance.origin.clone(),
            };
            validate_domain_effect_record_provenance(&record, provenance)?;
            Ok(Some(record))
        }
        _ => Ok(None),
    }
}

fn validate_domain_effect_record_provenance(
    record: &EffectRecord,
    provenance: &EffectProvenance,
) -> Result<(), EffectError> {
    if provenance.cursor != record.cursor {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance cursor does not match domain effect record cursor".to_string(),
        ));
    }
    if provenance.descriptor_hash != record.descriptor_hash {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance descriptor_hash does not match domain effect record descriptor_hash"
                .to_string(),
        ));
    }
    if provenance.descriptor != record.descriptor {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance descriptor does not match domain effect record descriptor"
                .to_string(),
        ));
    }

    let expected_group_id = effect_outcome_group_id(&record.cursor);
    if provenance.group_id.as_ref() != Some(&expected_group_id) {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect_provenance group_id does not match deterministic group id `{expected_group_id}`"
        )));
    }
    let EffectOutcomePayload::SucceededFact {
        outcome_fact_ordinal,
        ..
    } = &record.outcome
    else {
        return Err(EffectError::EffectProvenanceMismatch(
            "domain effect outcome facts must use SucceededFact records".to_string(),
        ));
    };
    if provenance.outcome_fact_ordinal != Some(*outcome_fact_ordinal) {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance outcome_fact_ordinal does not match domain effect record ordinal"
                .to_string(),
        ));
    }

    Ok(())
}

fn validate_effect_record_provenance(
    event_type: &str,
    record: &EffectRecord,
    provenance: &EffectProvenance,
) -> Result<(), EffectError> {
    let expected_event_type = framework_effect_event_type(&record.descriptor.effect_type);
    if event_type != expected_event_type {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "reserved framework effect event type `{event_type}` does not match record descriptor `{}` (expected `{expected_event_type}`)",
            record.descriptor.effect_type
        )));
    }

    if provenance.cursor != record.cursor {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance cursor does not match effect record cursor".to_string(),
        ));
    }
    if provenance.descriptor_hash != record.descriptor_hash {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance descriptor_hash does not match effect record descriptor_hash"
                .to_string(),
        ));
    }
    if provenance.descriptor != record.descriptor {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance descriptor does not match effect record descriptor".to_string(),
        ));
    }

    let expected_group_id = effect_outcome_group_id(&record.cursor);
    if provenance.group_id.as_ref() != Some(&expected_group_id) {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect_provenance group_id does not match deterministic group id `{expected_group_id}`"
        )));
    }
    if provenance.outcome_fact_ordinal.is_some() {
        return Err(EffectError::EffectProvenanceMismatch(
            "framework effect record compatibility facts must not set outcome_fact_ordinal"
                .to_string(),
        ));
    }

    Ok(())
}

pub(super) fn validate_effect_outcome_group(records: &[&EffectRecord]) -> Result<(), EffectError> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    // Non-fact outcomes (legacy single payload, or a failure) are singletons.
    // A multi-fact domain group declares its cardinality on every fact.
    let expected = match &first.outcome {
        EffectOutcomePayload::Succeeded { .. } | EffectOutcomePayload::Failed { .. } => {
            if records.len() != 1 {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "effect outcome group for cursor {:?} mixes a non-fact outcome with other records",
                    first.cursor
                )));
            }
            return Ok(());
        }
        EffectOutcomePayload::SucceededFact {
            outcome_fact_count, ..
        } => usize::try_from(outcome_fact_count.get()).map_err(|_| {
            EffectError::EffectProvenanceMismatch(format!(
                "effect outcome fact count for cursor {:?} exceeds usize range",
                first.cursor
            ))
        })?,
    };
    if expected == 0 {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect outcome group for cursor {:?} declares zero facts",
            first.cursor
        )));
    }

    let cursor = first.cursor.clone();
    let descriptor_hash = first.descriptor_hash.clone();
    let descriptor = first.descriptor.clone();
    // FLOWIP-120q: size the presence check to the recorded group cardinality,
    // not to the number of records present, so a group missing its top fact is
    // detected instead of passing as a smaller complete group.
    let mut seen = vec![false; expected];
    for record in records {
        if record.cursor != cursor {
            return Err(EffectError::EffectProvenanceMismatch(
                "effect outcome group contains multiple cursors".to_string(),
            ));
        }
        if record.descriptor_hash != descriptor_hash {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect outcome group for cursor {cursor:?} contains multiple descriptor hashes"
            )));
        }
        if record.descriptor != descriptor {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect outcome group for cursor {cursor:?} contains multiple descriptors"
            )));
        }

        let EffectOutcomePayload::SucceededFact {
            outcome_fact_ordinal,
            outcome_fact_count,
            ..
        } = &record.outcome
        else {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "multi-fact effect outcome group for cursor {cursor:?} contains a non-domain-success record"
            )));
        };
        if usize::try_from(outcome_fact_count.get()).unwrap_or(usize::MAX) != expected {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect outcome group for cursor {cursor:?} disagrees on outcome_fact_count"
            )));
        }
        let ordinal = usize::try_from(outcome_fact_ordinal.get()).map_err(|_| {
            EffectError::EffectProvenanceMismatch(format!(
                "effect outcome fact ordinal for cursor {cursor:?} exceeds usize range"
            ))
        })?;
        let Some(slot) = seen.get_mut(ordinal) else {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect outcome group for cursor {cursor:?} has ordinal {outcome_fact_ordinal} beyond declared count {expected}"
            )));
        };
        if *slot {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "effect outcome group for cursor {cursor:?} has duplicate ordinal {outcome_fact_ordinal}"
            )));
        }
        *slot = true;
    }

    let present_count = seen.iter().filter(|flag| **flag).count();
    if present_count == expected {
        return Ok(());
    }
    // Incomplete. A contiguous prefix {0..present-1} is a tolerated torn tail
    // that dropped the top ordinals, so the whole group is treated as absent. A
    // gap in the middle cannot come from a torn tail and is corruption.
    if seen[..present_count].iter().all(|flag| *flag) {
        return Err(EffectError::IncompleteOutcomeGroup {
            cursor,
            expected,
            present: present_count,
        });
    }
    Err(EffectError::EffectProvenanceMismatch(format!(
        "effect outcome group for cursor {cursor:?} is missing interior outcome fact ordinals"
    )))
}

pub(super) fn decode_effect_outcome_group<T>(records: &[&EffectRecord]) -> Result<T, EffectError>
where
    T: TypedFactSet,
{
    validate_effect_outcome_group(records)?;
    let [single] = records else {
        let mut ordered = records.to_vec();
        ordered.sort_by_key(|record| match &record.outcome {
            EffectOutcomePayload::SucceededFact {
                outcome_fact_ordinal,
                ..
            } => outcome_fact_ordinal.get(),
            _ => u32::MAX,
        });
        let facts = ordered
            .iter()
            .map(|record| match &record.outcome {
                EffectOutcomePayload::SucceededFact {
                    event_type, output, ..
                } => Ok(TypedFact {
                    event_type: event_type.clone(),
                    payload: output.clone(),
                }),
                _ => Err(EffectError::EffectProvenanceMismatch(
                    "multi-fact effect outcome group contains a non-domain-success record"
                        .to_string(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        return T::try_from_facts(&facts).map_err(effect_fact_set_error);
    };

    match &single.outcome {
        EffectOutcomePayload::SucceededFact {
            event_type, output, ..
        } => T::try_from_facts(&[TypedFact {
            event_type: event_type.clone(),
            payload: output.clone(),
        }])
        .map_err(effect_fact_set_error),
        EffectOutcomePayload::Succeeded { output } => {
            let fact_types = T::fact_types();
            let [fact_type] = fact_types.as_slice() else {
                return Err(EffectError::EffectProvenanceMismatch(
                    "legacy single-payload effect success cannot reconstruct a multi-fact output"
                        .to_string(),
                ));
            };
            T::try_from_facts(&[TypedFact {
                event_type: fact_type.event_type.clone(),
                payload: output.clone(),
            }])
            .map_err(effect_fact_set_error)
        }
        EffectOutcomePayload::Failed { .. } => recorded_failure_from_outcome(&single.outcome),
    }
}

#[derive(Debug)]
pub(super) enum EffectRecordMaterialization {
    DomainFacts {
        facts: Vec<TypedFact>,
        /// Origin read back from recorded provenance. `None` on older
        /// journals, where replay defaults to the effect itself.
        origin: Option<EffectFactOrigin>,
    },
    FrameworkRecords(Vec<EffectRecord>),
}

/// The commit path stamps one origin per outcome group, so records inside a
/// group that disagree on origin are corruption.
fn group_origin(records: &[&EffectRecord]) -> Result<Option<EffectFactOrigin>, EffectError> {
    let mut origins = records.iter().map(|record| &record.origin);
    let first = origins.next().cloned().flatten();
    if origins.any(|origin| origin.as_ref() != first.as_ref()) {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect outcome group records disagree on fact origin".to_string(),
        ));
    }
    Ok(first)
}

pub(super) fn effect_record_group_materialization(
    records: &[&EffectRecord],
) -> Result<EffectRecordMaterialization, EffectError> {
    validate_effect_outcome_group(records)?;
    let [single] = records else {
        let origin = group_origin(records)?;
        let mut ordered = records.to_vec();
        ordered.sort_by_key(|record| match &record.outcome {
            EffectOutcomePayload::SucceededFact {
                outcome_fact_ordinal,
                ..
            } => outcome_fact_ordinal.get(),
            _ => u32::MAX,
        });
        let mut facts = Vec::new();
        for record in ordered {
            let EffectOutcomePayload::SucceededFact {
                event_type, output, ..
            } = &record.outcome
            else {
                return Err(EffectError::EffectProvenanceMismatch(
                    "multi-fact effect outcome group contains a non-domain-success record"
                        .to_string(),
                ));
            };
            facts.push(TypedFact {
                event_type: event_type.clone(),
                payload: output.clone(),
            });
        }
        return Ok(EffectRecordMaterialization::DomainFacts { facts, origin });
    };

    match &single.outcome {
        EffectOutcomePayload::SucceededFact {
            event_type, output, ..
        } => Ok(EffectRecordMaterialization::DomainFacts {
            facts: vec![TypedFact {
                event_type: event_type.clone(),
                payload: output.clone(),
            }],
            origin: single.origin.clone(),
        }),
        EffectOutcomePayload::Succeeded { .. } | EffectOutcomePayload::Failed { .. } => {
            Ok(EffectRecordMaterialization::FrameworkRecords(vec![
                (*single).clone(),
            ]))
        }
    }
}

pub(super) fn is_routable_output_fact(
    output_contract: Option<&StageOutputContract>,
    event_type: &str,
) -> bool {
    match output_contract {
        Some(contract) if !contract.is_empty() => contract.is_routable_event_type(event_type),
        _ => false,
    }
}

/// FLOWIP-120a: decode a committed or recorded effect outcome into the effect's
/// typed output. Replay and the live transactional commit share this one decode
/// path, so a live transactional return value is byte-identical to what a replay
/// reconstructs from the same record.
pub(super) fn decode_effect_outcome<T>(outcome: &EffectOutcomePayload) -> Result<T, EffectError>
where
    T: DeserializeOwned,
{
    match outcome {
        EffectOutcomePayload::Succeeded { output } => serde_json::from_value(output.clone())
            .map_err(|e| EffectError::Serialization(e.to_string())),
        EffectOutcomePayload::SucceededFact { output, .. } => {
            serde_json::from_value(output.clone())
                .map_err(|e| EffectError::Serialization(e.to_string()))
        }
        EffectOutcomePayload::Failed {
            error_type,
            error_message,
            retry,
            cause,
            detail,
        } => {
            validate_failure_detail(error_type, detail.as_ref())?;
            Err(EffectError::RecordedFailure {
                error_type: error_type.clone(),
                error_message: error_message.clone(),
                retry: *retry,
                cause: cause.clone(),
                detail: detail.clone(),
            })
        }
    }
}

pub(super) fn recorded_failure_from_outcome<T>(
    outcome: &EffectOutcomePayload,
) -> Result<T, EffectError> {
    match outcome {
        EffectOutcomePayload::Failed {
            error_type,
            error_message,
            retry,
            cause,
            detail,
        } => {
            validate_failure_detail(error_type, detail.as_ref())?;
            Err(EffectError::RecordedFailure {
                error_type: error_type.clone(),
                error_message: error_message.clone(),
                retry: *retry,
                cause: cause.clone(),
                detail: detail.clone(),
            })
        }
        _ => Err(EffectError::EffectProvenanceMismatch(
            "expected recorded effect failure".to_string(),
        )),
    }
}

fn validate_failure_detail(
    error_type: &EffectFailureKind,
    detail: Option<&EffectFailureDetail>,
) -> Result<(), EffectError> {
    let invariant_type = error_type.as_str() == "effect_port_binding_invariant_violation";
    let invariant_detail = matches!(
        detail,
        Some(EffectFailureDetail::PortBindingInvariantViolation { .. })
    );
    if invariant_type != invariant_detail {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect port-binding invariant failure type/detail pairing is invalid".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn effect_fact_set_error(error: TypedFactSetError) -> EffectError {
    match error {
        TypedFactSetError::SerializationFailed(message) => EffectError::Serialization(message),
        TypedFactSetError::DeserializationFailed { event_type, error } => {
            EffectError::Serialization(format!("{event_type}: {error}"))
        }
        TypedFactSetError::MissingFact { event_type } => EffectError::EffectProvenanceMismatch(
            format!("effect outcome group is missing fact `{event_type}`"),
        ),
        TypedFactSetError::DuplicateFact { event_type } => EffectError::EffectProvenanceMismatch(
            format!("effect outcome group has duplicate fact `{event_type}`"),
        ),
        TypedFactSetError::UnexpectedFact { event_type } => EffectError::EffectProvenanceMismatch(
            format!("effect outcome group contains unexpected fact `{event_type}`"),
        ),
    }
}
