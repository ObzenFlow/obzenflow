// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Current-format journal fixture transformations for preservation proofs.
//! Physical codec details stay in Infra, including test-only archive rewrites.

use crate::journal::disk::codec::{self, Decoder, DefinitionStore};
use crate::journal::disk::scanner::{
    classify_frame, dispose, read_frame_sync, Disposition, ReadPolicy,
};
use obzenflow_core::event::{ChainEvent, JournalEvent, SystemEvent};
use obzenflow_core::journal::RunManifest;
use std::io::BufReader;
use std::path::Path;

/// Re-encode a sealed test archive with selected optional observations removed.
/// Decode every source before replacing any carrier, since another journal may
/// reference it. Each replacement has complete local definitions and preserves
/// original physical grouping, full provenance and business/protected payloads.
pub fn omit_observations(
    run: &Path,
    keep: impl Fn(usize) -> bool,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let manifest: RunManifest =
        serde_json::from_slice(&std::fs::read(run.join("run_manifest.json"))?)?;
    let mut files = std::collections::BTreeMap::new();
    files.insert(manifest.system_journal_file, true);
    for stage in manifest.stages.values() {
        files.insert(stage.data_journal_file.clone(), false);
        files.insert(stage.error_journal_file.clone(), false);
    }
    let mut ordinal = 0;
    let mut removed = 0;
    let mut replacements = Vec::new();
    for (file, system) in files {
        let path = run.join(file);
        if !path.exists() {
            continue;
        }
        let bytes = if system {
            rewrite::<SystemEvent>(&path, &keep, &mut ordinal, &mut removed)?
        } else {
            rewrite::<ChainEvent>(&path, &keep, &mut ordinal, &mut removed)?
        };
        replacements.push((path, bytes));
    }
    for (path, bytes) in replacements {
        std::fs::write(path, bytes)?;
    }
    Ok(removed)
}

fn rewrite<T: JournalEvent>(
    path: &Path,
    keep: &impl Fn(usize) -> bool,
    ordinal: &mut usize,
    removed: &mut usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    let mut decoder = Decoder::cold(path);
    let mut input = Vec::new();
    let mut output = Vec::new();
    let mut offset = 0;
    while let Some((consumed, termination)) = read_frame_sync(&mut reader, &mut input)? {
        let frame = match dispose(
            classify_frame::<T>(&input, &mut decoder, offset),
            termination,
            ReadPolicy::SealedScan {
                tolerate_torn_tail: false,
            },
        ) {
            Disposition::Yield(frame) => frame,
            Disposition::Corrupt(problem) => return Err(problem.to_string().into()),
            _ => return Err("unexpected uncommitted fixture".into()),
        };
        offset += consumed as u64;
        let group = frame.group_id().map(str::to_owned);
        let mut records = frame.into_records();
        for record in &mut records {
            let protected = serde_json::to_value(&record.envelope.provenance)?;
            let payload = serde_json::to_value(&record.payload)?;
            if !keep(*ordinal) {
                *removed += usize::from(record.envelope.observability.take().is_some());
            }
            *ordinal += 1;
            assert_eq!(
                serde_json::to_value(&record.envelope.provenance)?,
                protected
            );
            assert_eq!(serde_json::to_value(&record.payload)?, payload);
        }
        let encoded = codec::prepare(&records, group.as_deref(), path, DefinitionStore::default())?;
        // Independently decode and compare every field, including observations.
        let decoded = decoder
            .decode::<T>(
                codec::frame::validate(&encoded.bytes).map_err(codec::frame::io_error)?,
                output.len() as u64,
            )?
            .into_records();
        assert_eq!(records.len(), decoded.len());
        for (before, after) in records.iter().zip(decoded) {
            assert_eq!(serde_json::to_value(before)?, serde_json::to_value(after)?);
        }
        output.extend(encoded.bytes);
    }
    Ok(output)
}

#[derive(Default, Debug, serde::Serialize)]
pub struct StorageAudit {
    pub records: u64,
    pub packets: u64,
    pub frames: u64,
    pub archive_bytes: u64,
    pub journal_bytes: u64,
    pub support_bytes: u64,
    pub provenance_bytes: u64,
    pub observability_bytes: u64,
    pub payload_bytes: u64,
    pub shared_bytes: u64,
    pub logical_provenance_bytes: u64,
    pub logical_observability_bytes: u64,
    pub logical_payload_bytes: u64,
    pub uncompressed_control_bytes: u64,
    pub definition_cache_hits: u64,
    pub definition_cache_misses: u64,
    pub definition_frames_read: u64,
    pub definition_bytes_read: u64,
    pub definition_cache_evictions: u64,
    pub definition_cache_peak_bytes: usize,
    pub ordinary_provenance: SizeDistribution,
    pub root_provenance: SizeDistribution,
    pub attached_observability: SizeDistribution,
    pub full_measurement_packets: SizeDistribution,
    pub snapshot_only_packets: SizeDistribution,
}

#[derive(Default, Debug, serde::Serialize)]
pub struct SizeDistribution {
    pub count: usize,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

impl SizeDistribution {
    fn from_samples(mut samples: Vec<f64>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_by(f64::total_cmp);
        let count = samples.len();
        Self {
            count,
            mean: samples.iter().sum::<f64>() / count as f64,
            p50: samples[(count - 1) * 50 / 100],
            p95: samples[(count - 1) * 95 / 100],
            p99: samples[(count - 1) * 99 / 100],
            max: samples[count - 1],
        }
    }
}

#[derive(Default)]
struct Samples {
    provenance: Vec<f64>,
    roots: Vec<f64>,
    observations: Vec<f64>,
    full: Vec<f64>,
    snapshots: Vec<f64>,
}

/// Attribute all physical bytes and compare complete re-encoded logical records
/// over the identical captured stream. This is an existing-suite proof helper,
/// not another runtime journal format or a selectable production codec.
pub fn audit_archive(run: &Path) -> Result<StorageAudit, Box<dyn std::error::Error + Send + Sync>> {
    let manifest: RunManifest =
        serde_json::from_slice(&std::fs::read(run.join("run_manifest.json"))?)?;
    let mut audit = StorageAudit::default();
    let mut samples = Samples::default();
    audit_file::<SystemEvent>(
        &run.join(&manifest.system_journal_file),
        &mut audit,
        &mut samples,
    )?;
    for stage in manifest.stages.values() {
        for file in [&stage.data_journal_file, &stage.error_journal_file] {
            let path = run.join(file);
            if path.exists() {
                audit_file::<ChainEvent>(&path, &mut audit, &mut samples)?;
            }
        }
    }
    audit.archive_bytes = directory_bytes(run)?;
    audit.support_bytes = audit
        .archive_bytes
        .checked_sub(audit.journal_bytes)
        .ok_or("journal paths overlap")?;
    assert_eq!(
        audit.journal_bytes,
        audit.provenance_bytes
            + audit.observability_bytes
            + audit.payload_bytes
            + audit.shared_bytes
    );
    audit.ordinary_provenance = SizeDistribution::from_samples(samples.provenance);
    audit.root_provenance = SizeDistribution::from_samples(samples.roots);
    audit.attached_observability = SizeDistribution::from_samples(samples.observations);
    audit.full_measurement_packets = SizeDistribution::from_samples(samples.full);
    audit.snapshot_only_packets = SizeDistribution::from_samples(samples.snapshots);
    Ok(audit)
}

fn directory_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total += if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(total)
}

fn audit_file<T: JournalEvent>(
    path: &Path,
    audit: &mut StorageAudit,
    samples: &mut Samples,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let started = std::time::Instant::now();
    let initial_frames = audit.frames;
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    let mut decoder = Decoder::cold(path);
    let mut bytes = Vec::new();
    let mut offset = 0;
    while let Some((consumed, _)) = read_frame_sync(&mut reader, &mut bytes)? {
        let body = codec::frame::validate(&bytes).map_err(codec::frame::io_error)?;
        let (frame, sizes) = decoder.decode_measured::<T>(body, offset)?;
        offset += consumed as u64;
        let group = frame.group_id().map(str::to_owned);
        let records = frame.into_records();
        let logical: Vec<_> = records
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()?;
        for original in &logical {
            // The complete current record is the uncompressed control. Decode
            // it as the typed JSONL consumer does and compare every field. Codec
            // re-encoding, cold references and primitive boundaries are covered
            // by the focused provider tests, without rewriting this archive a
            // second time during a scale proof.
            let control = serde_json::to_vec(original)?;
            let restored: crate::journal::disk::log_record::LogRecord<T> =
                serde_json::from_slice(&control)?;
            assert_eq!(
                control,
                serde_json::to_vec(&serde_json::to_value(restored)?)?
            );
        }
        audit.frames += 1;
        audit.records += sizes.records as u64;
        audit.packets += sizes.packets as u64;
        audit.journal_bytes += consumed as u64;
        audit.provenance_bytes += sizes.provenance as u64;
        audit.observability_bytes += sizes.observability as u64;
        audit.payload_bytes += sizes.payload as u64;
        audit.shared_bytes += sizes.shared as u64;
        let control = match group {
            Some(group) => serde_json::json!({"group_id": group, "records": logical}),
            None => serde_json::json!({"record": logical[0]}),
        };
        audit.uncompressed_control_bytes += serde_json::to_vec(&control)?.len() as u64 + 32;
        // Shared bytes are split equally between populated envelope sections.
        // No absent packet is included in an attached-packet mean.
        let observation_shared = if sizes.packets == 0 {
            0.0
        } else {
            sizes.shared as f64 / 2.0
        };
        let p = (sizes.provenance as f64 + sizes.shared as f64 - observation_shared)
            / sizes.records as f64;
        let o = if sizes.packets == 0 {
            0.0
        } else {
            (sizes.observability as f64 + observation_shared) / sizes.packets as f64
        };
        for record in &logical {
            let provenance = &record["envelope"]["provenance"];
            let event = &provenance["event"];
            audit.logical_provenance_bytes += serde_json::to_vec(provenance)?.len() as u64;
            audit.logical_payload_bytes += serde_json::to_vec(&record["payload"])?.len() as u64;
            if let Some(packet) = record["envelope"].get("observability") {
                audit.logical_observability_bytes += serde_json::to_vec(packet)?.len() as u64;
                samples.observations.push(o);
                if packet.get("runtime").is_some() {
                    samples.full.push(o);
                } else if packet.get("runtime_snapshot").is_some() {
                    samples.snapshots.push(o);
                }
            }
            if sizes.records == 1
                && matches!(event["event_kind"].as_str(), Some("fact" | "delivery"))
                && event["causality"]["parent_ids"]
                    .as_array()
                    .is_some_and(|parents| parents.len() <= 1)
                && event.get("effect_provenance").is_none()
                && event["composite_activations"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
                && provenance["journal"]["vector_clock"]["clocks"]
                    .as_object()
                    .is_some_and(|clocks| clocks.len() <= 8)
            {
                if event["causality"]["parent_ids"].as_array().unwrap().len() == 1 {
                    samples.provenance.push(p);
                } else {
                    samples.roots.push(p);
                }
            }
        }
    }
    let cache = decoder.cache_stats();
    audit.definition_cache_hits += cache.hits;
    audit.definition_cache_misses += cache.misses;
    audit.definition_frames_read += cache.carrier_frames;
    audit.definition_bytes_read += cache.carrier_bytes;
    audit.definition_cache_evictions += cache.evictions;
    audit.definition_cache_peak_bytes = audit
        .definition_cache_peak_bytes
        .max(cache.peak_retained_bytes);
    println!(
        "Storage audit: {} frames in {:.3}s from {}",
        audit.frames - initial_frames,
        started.elapsed().as_secs_f64(),
        path.display()
    );
    Ok(())
}
