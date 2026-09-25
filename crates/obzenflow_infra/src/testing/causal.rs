// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Exact causal proof from admitted journal snapshots, independent of the
//! application replay projection (which deliberately excludes physical clocks).

use obzenflow_core::event::CommittedCausalRef;
use obzenflow_core::journal::causal::{CausalProof, CausalProofCache};
use obzenflow_core::journal::read::RunRecordData;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Default)]
pub struct CausalArchiveProof {
    pub valid: usize,
    pub chain_to_system: usize,
    pub system_to_chain: usize,
    pub witnessed_contract_results: usize,
    pub unresolved: Vec<(CommittedCausalRef, CausalProof)>,
    pub invalid: Vec<(CommittedCausalRef, CausalProof)>,
}

/// Include replay origins explicitly. No filesystem search, topology inference,
/// recursive ancestor materialisation or live runtime lookup supplies evidence.
/// A finite record budget bounds the admitted cuts; the component budget bounds
/// the resolver and reports evicted/missing references as unresolved.
pub async fn prove_archives(
    paths: &[&Path],
    max_records: usize,
    max_components: usize,
) -> Result<CausalArchiveProof, Box<dyn std::error::Error + Send + Sync>> {
    let mut records = Vec::new();
    let mut system_journals = BTreeMap::new();
    let mut resolver = CausalProofCache::new(max_records, max_components);
    for path in paths {
        let mut snapshot = crate::journal::read::open_disk_run(path).await?;
        while let Some(record) = snapshot.next().await? {
            if records.len() == max_records {
                return Err("causal archive record budget exhausted".into());
            }
            resolver.admit_run_record(&record)?;
            system_journals.insert(
                record.journal.id,
                matches!(record.record, RunRecordData::System(_)),
            );
            records.push(record);
        }
    }
    let mut report = CausalArchiveProof::default();
    for record in records {
        let reference = record.causal_commit()?.reference;
        match resolver.verify_run_record(&record) {
            CausalProof::Valid { resolved, .. } => {
                report.valid += 1;
                let system = matches!(record.record, RunRecordData::System(_));
                let cross_family = resolved.iter().any(|witness| {
                    system_journals
                        .get(witness.reference.journal_writer_id.as_journal_id())
                        .is_some_and(|parent_system| *parent_system != system)
                });
                if cross_family {
                    if system {
                        report.chain_to_system += 1;
                        if matches!(&record.record, RunRecordData::System(row)
                            if matches!(row.payload, obzenflow_core::event::SystemPayload::ContractResult { .. }))
                        {
                            report.witnessed_contract_results += 1;
                        }
                    } else {
                        report.system_to_chain += 1;
                    }
                }
            }
            proof @ CausalProof::Unresolved { .. } => report.unresolved.push((reference, proof)),
            proof @ CausalProof::Invalid { .. } => report.invalid.push((reference, proof)),
        }
    }
    Ok(report)
}
