// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Current-format journal fixture transformations for preservation proofs.
//! Physical codec details stay in Infra, including test-only archive rewrites.

use crate::journal::disk::codec::{self, Decoder, DefinitionStore};
use crate::journal::disk::inspect::load_manifest;
use crate::journal::disk::scanner::{
    classify_frame, dispose, read_frame_sync, Disposition, ReadPolicy,
};
use obzenflow_core::event::{ChainEvent, JournalEvent, SystemEvent};
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
    rewrite_archive(run, keep, |_, _| true).map(|(observations, _)| observations)
}

/// Retain complete frames in a closed test archive. The predicate receives the
/// journal path and expanded records so tests need not parse the wire format.
/// All journals are decoded before any carrier is replaced, and retained frames
/// are re-encoded with local definitions. Atomic groups cannot be split.
pub fn retain_archive_frames(
    run: &Path,
    keep: impl Fn(&Path, &[serde_json::Value]) -> bool,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    rewrite_archive(run, |_| true, keep).map(|(_, records)| records)
}

/// Damage exactly one non-final chain frame in a closed test archive without
/// changing its length or subsequent offsets. First localise all definitions so
/// the selected corruption cannot invalidate another journal's shared carrier.
pub fn corrupt_chain_frame(
    run: &Path,
    journal: &Path,
    select: impl Fn(&[serde_json::Value]) -> bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    retain_archive_frames(run, |_, _| true)?;
    let mut reader = BufReader::new(std::fs::File::open(journal)?);
    let mut decoder = Decoder::cold(journal);
    let mut input = Vec::new();
    let mut offset = 0;
    let mut selected = None;
    while let Some((consumed, termination)) = read_frame_sync(&mut reader, &mut input)? {
        let frame = match dispose(
            classify_frame::<ChainEvent>(&input, &mut decoder, offset),
            termination,
            ReadPolicy::SealedScan {
                tolerate_torn_tail: false,
            },
        ) {
            Disposition::Yield(frame) => frame,
            Disposition::Corrupt(problem) => return Err(problem.to_string().into()),
            _ => return Err("unexpected uncommitted fixture".into()),
        };
        let records = frame
            .into_records()
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        if select(&records) {
            if selected.is_some() {
                return Err("corruption predicate must select exactly one frame".into());
            }
            selected = Some((offset, offset + consumed as u64));
        }
        offset += consumed as u64;
    }
    let (start, end) = selected.ok_or("corruption predicate did not select a frame")?;
    if end == offset {
        return Err("corruption fixture must have a complete frame after the damaged frame".into());
    }
    let mut bytes = std::fs::read(journal)?;
    bytes[start as usize + codec::frame::HEADER_LEN] ^= 1;
    std::fs::write(journal, bytes)?;
    Ok(())
}

fn rewrite_archive(
    run: &Path,
    keep_observation: impl Fn(usize) -> bool,
    keep_frame: impl Fn(&Path, &[serde_json::Value]) -> bool,
) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    let manifest = load_manifest(run)?;
    let mut files = std::collections::BTreeMap::new();
    files.insert(manifest.system_journal_file, true);
    for stage in manifest.stages.values() {
        files.insert(stage.data_journal_file.clone(), false);
        files.insert(stage.error_journal_file.clone(), false);
    }
    let mut ordinal = 0;
    let mut removed = 0;
    let mut removed_records = 0;
    let mut replacements = Vec::new();
    for (file, system) in files {
        let path = run.join(file);
        if !path.exists() {
            continue;
        }
        let bytes = if system {
            rewrite::<SystemEvent>(
                &path,
                &keep_observation,
                &keep_frame,
                &mut ordinal,
                &mut removed,
                &mut removed_records,
            )?
        } else {
            rewrite::<ChainEvent>(
                &path,
                &keep_observation,
                &keep_frame,
                &mut ordinal,
                &mut removed,
                &mut removed_records,
            )?
        };
        replacements.push((path, bytes));
    }
    for (path, bytes) in replacements {
        std::fs::write(path, bytes)?;
    }
    Ok((removed, removed_records))
}

fn rewrite<T: JournalEvent>(
    path: &Path,
    keep: &impl Fn(usize) -> bool,
    keep_frame: &impl Fn(&Path, &[serde_json::Value]) -> bool,
    ordinal: &mut usize,
    removed: &mut usize,
    removed_records: &mut usize,
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
        let expanded = records
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        if !keep_frame(path, &expanded) {
            *removed_records += records.len();
            *ordinal += records.len();
            continue;
        }
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
    /// Kind order: writer, context, origin, descriptor, capture scope, clock keys, journal writer.
    pub inline_definition_counts: [u64; 7],
    pub inline_definition_bytes: [u64; 7],
    pub definition_reference_bytes: u64,
    pub ordinary_provenance: SizeDistribution,
    pub root_provenance: SizeDistribution,
    pub attached_observability: SizeDistribution,
    pub full_measurement_packets: SizeDistribution,
    pub snapshot_only_packets: SizeDistribution,
    pub atomic_group_envelope: SizeDistribution,
    pub effect_record_envelope: SizeDistribution,
    pub composite_record_envelope: SizeDistribution,
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
    groups: Vec<f64>,
    effects: Vec<f64>,
    composites: Vec<f64>,
}

/// Attribute all physical bytes and compare complete re-encoded logical records
/// over the identical captured stream. This is an existing-suite proof helper,
/// not another runtime journal format or a selectable production codec.
pub fn audit_archive(run: &Path) -> Result<StorageAudit, Box<dyn std::error::Error + Send + Sync>> {
    let manifest = load_manifest(run)?;
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
    audit.atomic_group_envelope = SizeDistribution::from_samples(samples.groups);
    audit.effect_record_envelope = SizeDistribution::from_samples(samples.effects);
    audit.composite_record_envelope = SizeDistribution::from_samples(samples.composites);
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
    let mut decode_time = std::time::Duration::ZERO;
    let mut roundtrip_time = std::time::Duration::ZERO;
    let mut accounting_time = std::time::Duration::ZERO;
    println!("Storage audit: starting {}", path.display());
    loop {
        let phase = std::time::Instant::now();
        let Some((consumed, _)) = read_frame_sync(&mut reader, &mut bytes)? else {
            break;
        };
        let body = codec::frame::validate(&bytes).map_err(codec::frame::io_error)?;
        let (frame, sizes) = decoder.decode_measured::<T>(body, offset)?;
        offset += consumed as u64;
        decode_time += phase.elapsed();
        let phase = std::time::Instant::now();
        let group = frame.group_id().map(str::to_owned);
        if group.is_some() {
            samples
                .groups
                .push((sizes.provenance + sizes.observability + sizes.shared) as f64);
        }
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
        roundtrip_time += phase.elapsed();
        let phase = std::time::Instant::now();
        audit.frames += 1;
        audit.records += sizes.records as u64;
        audit.packets += sizes.packets as u64;
        audit.journal_bytes += consumed as u64;
        audit.provenance_bytes += sizes.provenance as u64;
        audit.observability_bytes += sizes.observability as u64;
        audit.payload_bytes += sizes.payload as u64;
        audit.shared_bytes += sizes.shared as u64;
        for index in 0..7 {
            audit.inline_definition_counts[index] += sizes.inline_definition_counts[index] as u64;
            audit.inline_definition_bytes[index] += sizes.inline_definition_bytes[index] as u64;
        }
        audit.definition_reference_bytes += sizes.definition_reference_bytes as u64;
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
            // These inclusive figures retain every activation and relationship;
            // they are not the ordinary-record population or a context-stripped
            // 512-byte estimate. Group members share their group's total cost.
            if event.get("effect_provenance").is_some() {
                samples.effects.push(p + o);
            }
            if event["composite_activations"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
            {
                samples.composites.push(p + o);
            }
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
        accounting_time += phase.elapsed();
        let frames = audit.frames - initial_frames;
        if frames.is_multiple_of(10_000) {
            println!(
                "Storage audit progress: {frames} frames, {offset} bytes, {:.3}s; decode={:.3}s, JSON roundtrip={:.3}s, accounting={:.3}s; {}",
                started.elapsed().as_secs_f64(),
                decode_time.as_secs_f64(),
                roundtrip_time.as_secs_f64(),
                accounting_time.as_secs_f64(),
                path.display(),
            );
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
        "Storage audit: {} frames in {:.3}s; decode={:.3}s, JSON roundtrip={:.3}s, accounting={:.3}s, carrier_frames={}, carrier_bytes={}, cache_evictions={}; from {}",
        audit.frames - initial_frames,
        started.elapsed().as_secs_f64(),
        decode_time.as_secs_f64(),
        roundtrip_time.as_secs_f64(),
        accounting_time.as_secs_f64(),
        cache.carrier_frames,
        cache.carrier_bytes,
        cache.evictions,
        path.display()
    );
    Ok(())
}
