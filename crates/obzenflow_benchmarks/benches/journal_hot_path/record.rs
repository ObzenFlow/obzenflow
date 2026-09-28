// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{fixtures, measure, timed, Census, Sample};
use criterion::{Criterion, Throughput};
use obzenflow_core::benchmark::validate_structure;
use obzenflow_core::journal::limits::record_bytes;
use std::cell::LazyCell;
use tokio::runtime::Runtime;

fn operation(f: &fixtures::RecordFixture, operation: &str) -> Sample {
    let record = &f.record;
    let journal = &record.envelope.provenance.journal;
    match operation {
        "canonical_bytes" => {
            let (bytes, sample) = timed(|| record_bytes(record).unwrap());
            assert_eq!(bytes, f.canonical_bytes);
            sample.expect_work("record_accounting_serializations", 1);
            sample.expect_work("structural_validations", 1);
            sample
        }
        "structural_validation" => {
            let (reference, sample) = timed(|| validate_structure(record).unwrap());
            assert_eq!(reference.event_id, *record.id());
            sample.expect_work("structural_validations", 1);
            sample.expect_work(
                "validated_clock_entries",
                journal.vector_clock.clocks.len() as u64,
            );
            sample
        }
        "clock_clone" => {
            let (clock, sample) = timed(|| journal.vector_clock.clone());
            assert_eq!(clock, journal.vector_clock);
            sample.expect_work("structural_validations", 0);
            sample
        }
        "clock_json_bytes" => {
            let expected = f.clock_bytes;
            let (bytes, sample) = timed(|| record_bytes(&journal.vector_clock).unwrap());
            assert_eq!(bytes, expected);
            sample.expect_work("clock_serializations", 1);
            sample
        }
        "frame_integrity_and_routing" => {
            let input =
                f.corpus
                    .reconstruction_input(1, record.envelope.provenance.event.clone(), false);
            let (count, sample) = timed(|| input.verify_and_route());
            assert_eq!(count, 1);
            sample.expect_work("verified_frames", 1);
            sample.expect_work("records_constructed", 0);
            sample
        }
        "payload_json" => {
            let input =
                f.corpus
                    .reconstruction_input(1, record.envelope.provenance.event.clone(), false);
            let member = input.member();
            let (payload, sample) = timed(|| member.payload());
            if sample.is_census() {
                assert_eq!(
                    serde_json::to_value(payload).unwrap(),
                    serde_json::to_value(&record.payload).unwrap()
                );
            }
            sample.expect_work("payload_json_decodes", 1);
            sample.expect_work("structural_validations", 0);
            sample
        }
        "compact_provenance_warm" | "compact_provenance_cold" => {
            let cold = operation.ends_with("_cold");
            let input =
                f.corpus
                    .reconstruction_input(1, record.envelope.provenance.event.clone(), cold);
            let mut member = input.member();
            let (metadata, sample) = timed(|| member.metadata());
            assert_eq!(metadata.clock, journal.vector_clock);
            // The previous reference lives in binary routing, outside this body.
            assert_eq!(metadata.timestamp, journal.timestamp);
            assert_eq!(
                serde_json::to_value(metadata.event).unwrap(),
                serde_json::to_value(&record.envelope.provenance.event).unwrap()
            );
            sample.expect_work("payload_json_decodes", 0);
            sample.expect_work("definition_carrier_reads", u64::from(cold));
            sample
        }
        _ => unreachable!(),
    }
}

pub fn bench(c: &mut Criterion, runtime: &Runtime, censuses: &mut Vec<Census>) {
    let fixtures: Vec<_> = fixtures::dimensions()
        .into_iter()
        .map(|d| {
            (
                d,
                LazyCell::new(move || runtime.block_on(fixtures::RecordFixture::build(d))),
            )
        })
        .collect();
    for (name, operations) in [
        ("record_accounting", &["canonical_bytes"][..]),
        (
            "causal_record_work",
            &["structural_validation", "clock_clone", "clock_json_bytes"][..],
        ),
        (
            "record_reconstruction",
            &[
                "frame_integrity_and_routing",
                "payload_json",
                "compact_provenance_warm",
                "compact_provenance_cold",
            ][..],
        ),
    ] {
        let mut group = c.benchmark_group(name);
        group.throughput(Throughput::Elements(1));
        for (dimensions, fixture) in &fixtures {
            for op in operations {
                let case = format!("{op}/{}", dimensions.name());
                let mut input = dimensions.json();
                if name == "record_reconstruction" {
                    input["definition_cache"] = serde_json::json!(if op.ends_with("_cold") {
                        "cold"
                    } else {
                        "warm"
                    });
                }
                let mut taken = false;
                group.bench_function(&case, |b| {
                    measure(
                        b,
                        censuses,
                        &mut taken,
                        &format!("{name}/{case}"),
                        &input,
                        || operation(fixture, op),
                    );
                });
            }
        }
        group.finish();
    }
}
