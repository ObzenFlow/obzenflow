// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Immutable physical commitment identity, including for empty journals.

use obzenflow_core::journal::{JournalError, JOURNAL_SCHEMA_VERSION};
use obzenflow_core::{FlowId, JournalId};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalIdentity {
    pub run_id: FlowId,
    pub journal_id: JournalId,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    schema: String,
    identity: JournalIdentity,
}

fn failure(
    message: impl Into<String>,
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> JournalError {
    JournalError::Implementation {
        message: message.into(),
        source: source.into(),
    }
}

pub fn read_identity(path: &Path) -> Result<JournalIdentity, JournalError> {
    let descriptor = path.with_extension("identity.json");
    if std::fs::metadata(&descriptor)
        .map_err(|error| failure("Inspect journal identity", error))?
        .len()
        > 4096
    {
        return Err(failure(
            "Journal identity exceeds descriptor budget",
            "oversized descriptor",
        ));
    }
    let bytes = std::fs::read(&descriptor).map_err(|error| {
        failure(
            format!("Read journal identity {}", descriptor.display()),
            error,
        )
    })?;
    let descriptor: Descriptor = serde_json::from_slice(&bytes)
        .map_err(|error| failure("Invalid journal identity", error))?;
    if descriptor.schema != JOURNAL_SCHEMA_VERSION {
        return Err(failure(
            "Unsupported journal identity schema",
            "incompatible journal schema",
        ));
    }
    Ok(descriptor.identity)
}

/// Bind every present journal, including empty ones, to its archive namespace.
pub(super) fn validate_archive_identities(
    directory: &Path,
    manifest: &obzenflow_core::journal::archive::manifest::RunManifest,
) -> Result<(), JournalError> {
    let expected_run = manifest
        .flow_id
        .parse::<FlowId>()
        .map_err(|error| failure("Invalid archive run identity", error))?;
    let files = std::iter::once(manifest.system_journal_file.as_str()).chain(
        manifest.stages.values().flat_map(|stage| {
            [
                stage.data_journal_file.as_str(),
                stage.error_journal_file.as_str(),
            ]
        }),
    );
    let files = files.chain(manifest.metrics_journals.iter().flat_map(|metrics| {
        [
            metrics.coordination_journal_file.as_str(),
            metrics.export_journal_file.as_str(),
        ]
    }));
    let mut identities = std::collections::HashSet::new();
    for file in files {
        let path = directory.join(file);
        if !path.exists() {
            continue;
        }
        let identity = read_identity(&path)?;
        if identity.run_id != expected_run || !identities.insert(identity.journal_id) {
            return Err(failure(
                "Conflicting archive journal identity",
                "journal namespace mismatch",
            ));
        }
    }
    Ok(())
}

/// The returned file owns a process-local OS write lock. Clones of a journal
/// retain that same lock; independent writable opens are rejected. Readers do
/// not acquire it. This is not a distributed writer lease.
pub(super) fn open_identity(
    path: &Path,
    run_id: Option<FlowId>,
) -> Result<(JournalIdentity, File), JournalError> {
    let file = File::options()
        .read(true)
        .append(true)
        .create(true)
        .open(path)
        .map_err(|error| failure("Open journal writer", error))?;
    file.try_lock()
        .map_err(|error| failure("Journal already has an independent writer", error))?;
    let descriptor = path.with_extension("identity.json");
    let identity = if descriptor.exists() {
        read_identity(path)?
    } else {
        if file
            .metadata()
            .map_err(|error| failure("Inspect journal", error))?
            .len()
            != 0
        {
            return Err(failure(
                "Journal has records but no immutable identity",
                "missing journal descriptor",
            ));
        }
        let identity = JournalIdentity {
            run_id: run_id.unwrap_or_default(),
            journal_id: JournalId::new(),
        };
        let bytes = serde_json::to_vec(&Descriptor {
            schema: JOURNAL_SCHEMA_VERSION.into(),
            identity,
        })
        .map_err(|error| failure("Encode journal identity", error))?;
        let mut output = File::options()
            .write(true)
            .create_new(true)
            .open(&descriptor)
            .map_err(|error| failure("Create journal identity", error))?;
        output
            .write_all(&bytes)
            .and_then(|()| output.sync_all())
            .map_err(|error| failure("Persist journal identity", error))?;
        if let Some(parent) = descriptor.parent() {
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| failure("Persist journal identity directory", error))?;
        }
        identity
    };
    if run_id.is_some_and(|expected| identity.run_id != expected) {
        return Err(failure(
            "Journal belongs to another run",
            "journal run identity mismatch",
        ));
    }
    Ok((identity, file))
}

/// Admission of a contiguous committed prefix, independent of record authorship.
/// A valid frame or a higher sequence alone cannot prove the intervening history.
#[derive(Clone)]
pub(crate) struct CommitmentAdmission {
    identity: JournalIdentity,
    previous: Option<obzenflow_core::event::CommittedCausalRef>,
}

impl CommitmentAdmission {
    pub(super) fn new(identity: JournalIdentity) -> Self {
        Self {
            identity,
            previous: None,
        }
    }

    pub(crate) fn open(path: &Path) -> Result<Self, JournalError> {
        Ok(Self::new(read_identity(path)?))
    }

    pub(crate) fn admit<P: obzenflow_core::event::payloads::JournalPayload>(
        &mut self,
        record: &obzenflow_core::JournalRecord<P>,
    ) -> Result<obzenflow_core::event::PreparedCausalCommit, JournalError> {
        let commitment = obzenflow_core::event::PreparedCausalCommit::from_record(record)?;
        let reference = commitment.reference;
        if reference.run_id != self.identity.run_id
            || reference.journal_writer_id.as_journal_id() != &self.identity.journal_id
            || record.envelope.provenance.journal.causal.previous != self.previous
        {
            return Err(obzenflow_core::event::CausalError::ConflictingCommitment.into());
        }
        self.previous = Some(reference);
        Ok(commitment)
    }
}

#[cfg(test)]
pub(super) fn write_fixture_identity(path: &Path, identity: JournalIdentity) {
    std::fs::write(
        path.with_extension("identity.json"),
        serde_json::to_vec(&Descriptor {
            schema: JOURNAL_SCHEMA_VERSION.into(),
            identity,
        })
        .unwrap(),
    )
    .unwrap();
}

#[cfg(test)]
pub(super) fn fixture_record<T: obzenflow_core::event::JournalEvent>(
    identity: JournalIdentity,
    event: T,
    previous: &mut Option<obzenflow_core::event::PreparedCausalCommit>,
) -> obzenflow_core::JournalRecord<T::Payload> {
    use obzenflow_core::event::{CausalCoordinate, CausalFrontier, PreparedCausalCommit};
    let (commitment, causal) = PreparedCausalCommit::prepare(
        identity.run_id,
        CausalCoordinate::new(identity.journal_id.into()),
        *event.id(),
        previous.as_ref(),
        &CausalFrontier::default(),
    )
    .unwrap();
    let record = obzenflow_core::JournalRecord::commit_event(
        event,
        obzenflow_core::event::provenance::JournalProvenance {
            run_id: identity.run_id,
            journal_writer_id: identity.journal_id.into(),
            causal,
            vector_clock: commitment.clock.clone(),
            timestamp: chrono::Utc::now(),
            journal_group_id: None,
            journal_group_member: None,
        },
    )
    .unwrap();
    *previous = Some(commitment);
    record
}
