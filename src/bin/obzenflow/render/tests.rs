// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use serde_json::json;

fn id(value: u64) -> String {
    format!("{value:026X}")
}

fn coordinate(stage: u64) -> obzenflow_core::event::CausalCoordinate {
    serde_json::from_value(
        json!({"journal_writer_id":id(stage),"writer_id":{"type":"Stage","id":id(stage)}}),
    )
    .unwrap()
}

fn displayed_clock(stage: u64, sequence: u64) -> String {
    let name = if stage == 1 {
        "thermometer"
    } else {
        "classify"
    };
    format!("⟨{name}@journal_{}:{sequence}⟩", id(stage))
}

fn fact(stage: u64, event: u64, parents: &[u64], payload: Value) -> RunRecord {
    let (key, stage_type, kind, event_type) = if stage == 1 {
        (
            "thermometer",
            "FiniteSource",
            "source_fact",
            "sensor.reading.v1",
        )
    } else {
        (
            "classify",
            "Transform",
            "stage_output",
            "sensor.classified.v1",
        )
    };
    serde_json::from_value(json!({
        "version": 2,
        "run": {"flow_id": id(10), "pipeline_writer_id": {"type":"System", "id":id(11)}},
        "journal": {"id":id(stage), "kind":"data", "stage":{"key":key, "id":id(stage), "stage_type":stage_type,"is_effectful":false}},
        "position":event,
        "kind":kind,
        "record": {
            "envelope": {"provenance": {
                "event": {
                    "id":id(event), "writer_id":{"type":"Stage", "id":id(stage)},
                    "event_kind":"fact", "event_type":event_type,
                    "causality":{"parent_ids":parents.iter().map(|p| id(*p)).collect::<Vec<_>>()},
                    "flow_context":{"flow_name":"sensors", "flow_id":id(10), "stage_name":key, "stage_id":id(stage), "stage_type":stage_type},
                    "processing":{"event_time":0, "status":"Success"}, "composite_activations":[]
                },
                "journal": {
                    "journal_writer_id":id(stage),
                    "run_id":id(10),
                    "causal":{"previous":{"run_id":id(10),"journal_writer_id":id(stage),"writer_id":{"type":"Stage","id":id(stage)},"sequence":event-1,"event_id":id(event-1)},"witnesses":[]},
                    "vector_clock":{"entries":[{"journal_writer_id":id(stage),"writer_id":{"type":"Stage","id":id(stage)},"sequence":event}]},
                    "timestamp":"2026-09-23T00:00:00Z", "journal_group_id":null, "journal_group_member":null
                }
            }},
            "payload":payload
        }
    })).unwrap()
}

fn renderer() -> Renderer {
    let source = fact(1, 100, &[], json!({}));
    let transform = fact(2, 101, &[100], json!({}));
    let mut renderer = Renderer::new(
        &ViewArgs::default(),
        false,
        false,
        [&source.journal, &transform.journal].into_iter(),
    );
    renderer.width = 160;
    renderer
}

#[test]
fn event_counts_keep_physical_journals_separate_for_the_same_event_type() {
    let source = fact(1, 100, &[], json!({"n": 1}));
    let mut forwarded = source.clone();
    forwarded.journal = fact(2, 101, &[100], json!({})).journal;
    let mut error = forwarded.clone();
    error.journal.id = serde_json::from_value(json!(id(3))).unwrap();
    error.journal.kind = RunJournalKind::Error;
    let mut counts = event_counts::EventCounts::default();
    for record in [&source, &source, &forwarded, &error] {
        counts.record(record);
    }
    assert_eq!(counts.journals().count(), 3);
    for (record, expected) in [(&source, 2), (&forwarded, 1), (&error, 1)] {
        let journal = counts
            .journals()
            .find(|counts| counts.journal.id == record.journal.id)
            .unwrap();
        assert_eq!(
            journal.event_types[&("sensor.reading.v1".into(), writer_id(record))],
            expected
        );
        assert_eq!(journal.omitted, 0);
    }
}

fn execution(event: u64, parents: &[u64], payload: ExecutionPayload) -> RunRecord {
    let mut record = fact(2, event, parents, json!({}));
    record.kind = if matches!(payload, ExecutionPayload::EffectRecord(_)) {
        RunRecordKind::Effect
    } else {
        RunRecordKind::Execution
    };
    if let RunRecordData::Chain(row) = &mut record.record {
        row.payload = ChainPayload::Execution(payload);
        row.envelope.provenance.event.event_kind = row.payload.kind();
        row.envelope.provenance.event.event_type =
            row.payload.framework_event_type().unwrap().into();
    }
    record
}

#[test]
fn human_and_jsonl_select_the_same_records() {
    let records = [
        fact(1, 100, &[], json!({"n": 1})),
        execution(
            102,
            &[100],
            ExecutionPayload::AccumulatorProgress {
                inputs_since_last_report: 1,
            },
        ),
        fact(2, 103, &[102], json!({"n": 2})),
    ];
    for include_runtime in [false, true] {
        for jsonl in [false, true] {
            let view = ViewArgs {
                jsonl,
                include_runtime,
                ..ViewArgs::default()
            };
            let mut renderer = Renderer::new(
                &view,
                false,
                false,
                records.iter().map(|record| &record.journal),
            );
            let mut output = Vec::new();
            for record in &records {
                renderer.record(&mut output, record.clone()).unwrap();
            }
            renderer.flush_pending(&mut output).unwrap();
            let expected = if include_runtime {
                vec![100, 102, 103]
            } else {
                vec![100, 103]
            };
            assert_eq!(renderer.records, 3, "hidden records are still consumed");
            assert_eq!(renderer.shown_records, expected.len() as u64);
            assert_eq!(
                renderer.shown_journals.values().sum::<u64>(),
                expected.len() as u64
            );
            let output = String::from_utf8(output).unwrap();
            if jsonl {
                let positions: Vec<u64> = output
                    .lines()
                    .map(|line| {
                        let record: RunRecord = serde_json::from_str(line).unwrap();
                        serde_json::to_value(record.position)
                            .unwrap()
                            .as_u64()
                            .unwrap()
                    })
                    .collect();
                assert_eq!(positions, expected);
            } else {
                assert_eq!(output.contains("RUNTIME"), include_runtime);
                assert!(output.contains("sensor.reading.v1 ←"));
                assert!(output.contains("sensor.classified.v1 ←"));
            }
        }
    }
}

#[test]
fn hidden_runtime_records_preserve_parent_resolution_and_separate_fact_groups() {
    let mut renderer = renderer();
    let mut output = Vec::new();
    let progress = execution(
        102,
        &[100],
        ExecutionPayload::AccumulatorProgress {
            inputs_since_last_report: 1,
        },
    );
    let progress_type = event_type(&progress).to_owned();
    for record in [
        fact(1, 100, &[], json!({"n":1})),
        fact(2, 101, &[100], json!({"n":1})),
        progress,
        fact(2, 103, &[100], json!({"n":2})),
        fact(2, 104, &[102], json!({"n":3})),
    ] {
        renderer.record(&mut output, record).unwrap();
    }
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(!text.contains("RUNTIME") && !text.contains("inputs_since_last_report"));
    assert_eq!(
        text.matches("TRANSFORM\nsensor.classified.v1 ← ").count(),
        3,
        "a hidden journal boundary must still separate facts: {text}"
    );
    assert!(!text.contains(&format!("\n{progress_type} ← ")));
    assert!(text.contains(&format!("sensor.classified.v1 ← classify({progress_type})")));
    assert!(
        !text.contains(&displayed_clock(2, 102)),
        "the hidden input clock is not repeated"
    );
    assert!(!text.contains("unresolved"));
    assert_eq!(renderer.records, 5);
    assert_eq!(renderer.shown_records, 4);
    assert!(!renderer.event_types.contains_key(&progress_type));
}

#[test]
fn default_keeps_failed_effect_evidence_while_verbose_runtime_stays_gray() {
    let failed = execution(
        101,
        &[100],
        ExecutionPayload::EffectRecord(serde_json::from_value(json!({
            "cursor":{"recorded_flow_id":id(10),"stage_key":"classify","input_seq":1,"effect_ordinal":0},
            "descriptor_hash":"fixture",
            "descriptor":{
                "effect_type":"sensor.calibrate","label":"calibrate","schema_version":1,
                "stage_logic_version":"v1","canonical_input_hash":"fixture","binding":{"mode":"portless"}
            },
            "outcome":{"outcome":"failed","error_type":"timeout","error_message":"calibration unavailable","retry":"retryable"}
        })).unwrap()),
    );
    let progress = execution(
        102,
        &[100],
        ExecutionPayload::AccumulatorProgress {
            inputs_since_last_report: 1,
        },
    );
    for verbose in [false, true] {
        let mut renderer = renderer();
        renderer.color = true;
        renderer.include_runtime = verbose;
        let mut output = Vec::new();
        for mut record in [
            fact(1, 100, &[], json!({"celsius":38})),
            failed.clone(),
            progress.clone(),
            fact(2, 103, &[100], json!({"reason":"unavailable"})),
        ] {
            let stage = record.journal.stage.as_mut().unwrap();
            stage.is_effectful = stage.key == "classify";
            renderer.record(&mut output, record).unwrap();
        }
        renderer.flush_pending(&mut output).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("\x1b[38;5;217mEFFECT\x1b[0m\n\x1b[38;5;217mobzenflow.effect_record.v1 ← classify(sensor.reading.v1)\x1b[0m"));
        assert!(
            text.contains("\"outcome\": \"failed\"")
                && text.contains("\"effect_type\": \"sensor.calibrate\"")
        );
        assert!(text.contains("\"error_message\": \"calibration unavailable\""));
        assert!(text.contains("\x1b[1;38;5;208mEFFECTFUL TRANSFORM\x1b[0m\n\x1b[1;38;5;208msensor.classified.v1 ← classify(sensor.reading.v1)\x1b[0m"));
        assert_eq!(text.contains("RUNTIME"), verbose);
        if verbose {
            let runtime = text
                .split("\x1b[38;5;245mRUNTIME")
                .nth(1)
                .unwrap()
                .split("\n\n")
                .next()
                .unwrap();
            assert!(runtime.contains("\x1b[38;5;245m  \"execution_type\":"));
            assert!(!runtime.contains("38;5;208m") && !runtime.contains("38;5;217m"));
        }
    }
}

#[test]
fn declared_effectful_stage_kinds_apply_before_any_effect_has_run() {
    for (stage_type, heading, color) in [
        (StageType::Transform, "EFFECTFUL TRANSFORM", 208),
        (StageType::Stateful, "EFFECTFUL STATEFUL", 114),
    ] {
        let mut renderer = renderer();
        renderer.color = true;
        let mut result = fact(2, 101, &[100], json!({"reason":"no_effect_needed"}));
        let stage = result.journal.stage.as_mut().unwrap();
        stage.stage_type = stage_type;
        stage.is_effectful = true;
        let mut output = Vec::new();
        renderer
            .record(&mut output, fact(1, 100, &[], json!({"celsius":38})))
            .unwrap();
        renderer.record(&mut output, result).unwrap();
        renderer.flush_pending(&mut output).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains(&format!(
            "\x1b[1;38;5;{color}m{heading}\x1b[0m\n\x1b[1;38;5;{color}msensor.classified.v1 ← classify(sensor.reading.v1)\x1b[0m"
        )), "{text}");
        assert!(
            !text.contains("mEFFECT\x1b"),
            "capability is not an effect invocation"
        );
    }
}

#[test]
fn current_projection_requires_declared_stage_capability() {
    for missing in [true, false] {
        let mut record = serde_json::to_value(fact(2, 101, &[], json!({}))).unwrap();
        let stage = record["journal"]["stage"].as_object_mut().unwrap();
        if missing {
            stage.remove("is_effectful");
        } else {
            stage.insert("is_effectful".into(), Value::Null);
        }
        assert!(serde_json::from_value::<RunRecord>(record).is_err());
    }
}

#[test]
fn stage_capability_comes_from_the_manifest_regardless_of_effect_provenance() {
    for (declared, effect_stage, expected) in [
        (true, "classify", "EFFECTFUL TRANSFORM"),
        (true, "upstream", "EFFECTFUL TRANSFORM"),
        (false, "classify", "TRANSFORM"),
        (false, "upstream", "TRANSFORM"),
    ] {
        let mut renderer = renderer();
        let mut result = fact(2, 101, &[100], json!({"result":"calibrated"}));
        result.journal.stage.as_mut().unwrap().is_effectful = declared;
        if let RunRecordData::Chain(row) = &mut result.record {
            row.envelope.provenance.event.effect_provenance = Some(serde_json::from_value(json!({
                "cursor":{"recorded_flow_id":id(10), "stage_key":effect_stage, "input_seq":1, "effect_ordinal":0},
                "descriptor_hash":"fixture",
                "descriptor":{
                    "effect_type":"sensor.calibrate", "label":"calibrate", "schema_version":1,
                    "stage_logic_version":"v1", "canonical_input_hash":"fixture", "binding":{"mode":"portless"}
                },
                "outcome_fact_ordinal":0, "outcome_fact_count":1
            })).unwrap());
        }
        let mut derived = fact(2, 102, &[100], json!({"notice":"calibration_complete"}));
        derived.journal.stage.as_mut().unwrap().is_effectful = declared;
        let mut output = Vec::new();
        for record in [fact(1, 100, &[], json!({"celsius":38})), result, derived] {
            renderer.record(&mut output, record).unwrap();
        }
        renderer.flush_pending(&mut output).unwrap();
        let text = String::from_utf8(output).unwrap();
        let headings: Vec<_> = text
            .split("\n\n")
            .filter_map(|block| block.lines().next())
            .collect();
        assert_eq!(headings, ["SOURCE", expected, expected]);
    }
}

#[test]
fn ninety_columns_keeps_long_output_expressions_together_and_narrow_views_still_wrap() {
    let expression =
        "payment.authorization_unavailable.v1 ← authorize_payment(payment.order_validated.v1)";
    for width in [80, 90] {
        let mut renderer = renderer();
        renderer.width = width;
        let mut input = fact(1, 100, &[], json!({}));
        if let RunRecordData::Chain(row) = &mut input.record {
            row.envelope.provenance.event.event_type = "payment.order_validated.v1".into();
        }
        let mut result = fact(2, 101, &[100], json!({"reason":"unavailable"}));
        let stage = result.journal.stage.as_mut().unwrap();
        stage.key = "authorize_payment".into();
        stage.is_effectful = true;
        if let RunRecordData::Chain(row) = &mut result.record {
            row.envelope.provenance.event.event_type =
                "payment.authorization_unavailable.v1".into();
        }
        let mut output = Vec::new();
        renderer.record(&mut output, input).unwrap();
        renderer.record(&mut output, result).unwrap();
        renderer.flush_pending(&mut output).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert_eq!(
            text.lines().any(|line| line == expression),
            width == 90,
            "{text}"
        );
        assert!(
            text.lines().all(|line| line.chars().count() <= width),
            "{text}"
        );
    }
}

#[test]
fn multiple_outputs_each_lead_with_their_own_type_clock_and_payload_after_late_parent() {
    let mut renderer = renderer();
    let mut output = Vec::new();
    let first = fact(2, 101, &[100], json!({"sensor":"A", "reason":"too_hot"}));
    let mut second = fact(
        2,
        102,
        &[100],
        json!({"sensor":"A", "reason":{"cooling":"requested"}}),
    );
    if let RunRecordData::Chain(row) = &mut second.record {
        row.envelope.provenance.event.event_type = "sensor.cooling_requested.v1".into();
    }
    renderer.record(&mut output, first).unwrap();
    renderer.record(&mut output, second).unwrap();
    assert!(
        output.is_empty(),
        "wait for the actual input, not a guessed type"
    );
    renderer
        .record(&mut output, fact(1, 100, &[], json!({"celsius":38})))
        .unwrap();
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.contains(&format!(
            "SOURCE\nsensor.reading.v1 ← thermometer()\n{}",
            displayed_clock(1, 100)
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "TRANSFORM\nsensor.classified.v1 ← classify(sensor.reading.v1)\n{}",
            displayed_clock(2, 101)
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "TRANSFORM\nsensor.cooling_requested.v1 ← classify(sensor.reading.v1)\n{}",
            displayed_clock(2, 102)
        )),
        "{text}"
    );
    let clocks: Vec<_> = text
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with('⟨'))
        .collect();
    assert_eq!(
        clocks,
        [
            displayed_clock(1, 100),
            displayed_clock(2, 101),
            displayed_clock(2, 102)
        ]
    );
    let last_clock = text.find(&displayed_clock(2, 102)).unwrap();
    let last_fact = text
        .find("sensor.cooling_requested.v1 ← classify(sensor.reading.v1)")
        .unwrap();
    assert!(
        last_fact < last_clock,
        "each grouped fact retains its own clock below its typed equation"
    );
    assert_eq!(
        text.matches("\"sensor\": \"A\"").count(),
        2,
        "each fact keeps its own payload, even when fields match: {text}"
    );
    for (event_type, reason) in [
        ("sensor.classified.v1", json!("too_hot")),
        (
            "sensor.cooling_requested.v1",
            json!({"cooling":"requested"}),
        ),
    ] {
        let block = text
            .split("\n\n")
            .find(|block| block.starts_with(&format!("TRANSFORM\n{event_type} ← ")))
            .unwrap();
        let payload_start = block.find("\n{\n").unwrap() + 1;
        let payload: Value = serde_json::from_str(&block[payload_start..]).unwrap();
        assert_eq!(payload, json!({"sensor":"A", "reason":reason}));
    }
    assert_eq!(
        renderer.records, 3,
        "grouped display still counts every record"
    );
}

#[test]
fn missing_parents_flush_without_inventing_input_types_or_grouping_unrelated_facts() {
    let mut renderer = renderer();
    let mut output = Vec::new();
    renderer
        .record(&mut output, fact(1, 100, &[], json!({"n":1})))
        .unwrap();
    renderer
        .record(&mut output, fact(2, 101, &[98], json!({"n":1})))
        .unwrap();
    renderer
        .record(&mut output, fact(2, 102, &[99], json!({"n":1})))
        .unwrap();
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert_eq!(
        text.matches("TRANSFORM\nsensor.classified.v1 ← ").count(),
        2,
        "{text}"
    );
    assert_eq!(
        text.lines()
            .filter(|line| matches!(*line, "SOURCE" | "TRANSFORM"))
            .count(),
        3,
        "matching values do not establish shared parents"
    );
    assert!(!text.contains("classify(sensor.reading.v1)"));
    assert_eq!(
        text.matches("sensor.classified.v1 ← classify(recorded input (unresolved))")
            .count(),
        2,
        "unknown inputs never replace the known output heading"
    );
}

#[test]
fn continuous_stream_with_missing_parents_has_bounded_pending_rows() {
    let mut renderer = renderer();
    let mut output = Vec::new();
    for n in 100..(100 + MAX_PENDING as u64 * 2) {
        renderer
            .record(&mut output, fact(2, n, &[9999], json!({"n":n})))
            .unwrap();
        assert!(renderer.pending.len() < MAX_PENDING);
    }
    renderer.flush_pending(&mut output).unwrap();
    assert_eq!(renderer.records, MAX_PENDING as u64 * 2);
    assert!(renderer.pending.is_empty());
}

#[test]
fn explanation_resolves_full_commitments_when_a_cross_journal_witness_arrives_later() {
    use obzenflow_core::event::{CausalCommit, CausalFrontier};

    let mut source = fact(1, 100, &[], json!({}));
    let RunRecordData::Chain(row) = &mut source.record else {
        unreachable!()
    };
    row.envelope.provenance.journal.causal = Default::default();
    row.envelope
        .provenance
        .journal
        .vector_clock
        .clocks
        .insert(coordinate(1), 1);
    let input = CausalFrontier::from_record(row).unwrap();
    let mut child = fact(2, 101, &[], json!({}));
    let RunRecordData::Chain(row) = &mut child.record else {
        unreachable!()
    };
    let (commitment, witnesses) =
        CausalCommit::prepare(child.run.flow_id, coordinate(2), *row.id(), None, &input).unwrap();
    row.envelope.provenance.journal.vector_clock = commitment.clock;
    row.envelope.provenance.journal.causal = witnesses;

    let mut renderer = renderer();
    renderer.explain = true;
    renderer.full = true;
    let mut output = Vec::new();
    renderer.record(&mut output, child).unwrap();
    assert!(
        output.is_empty(),
        "bounded buffering can still resolve this witness"
    );
    renderer.record(&mut output, source).unwrap();
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert_eq!(text.matches("\"status\":\"valid\"").count(), 2, "{text}");
    assert!(text.contains("\"resolved\":[{\"reference\":"), "{text}");
    assert!(text.contains("\"merged\":{\"entries\":[{"), "{text}");
    assert!(!text.contains("\"status\":\"unresolved\""), "{text}");
}

#[test]
fn clocks_keep_unknown_writers_and_do_not_derive_causality_from_dominance() {
    let mut renderer = renderer();
    let mut source = fact(1, 100, &[], json!({}));
    if let RunRecordData::Chain(row) = &mut source.record {
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(coordinate(3), 12);
    }
    let mut output = Vec::new();
    renderer.record(&mut output, source).unwrap();
    renderer
        .record(&mut output, fact(2, 101, &[], json!({})))
        .unwrap();
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.contains(&format!(
            "⟨thermometer@journal_{}:100,{}@journal_{}:12⟩",
            id(1),
            coordinate(3).writer_id,
            id(3)
        )),
        "{text}"
    );
    assert!(text.contains("input not recorded"), "{text}");
}

#[test]
fn compact_clock_rows_follow_output_equations_without_truncating_counts() {
    let mut renderer = renderer();
    renderer.width = 60;
    let mut output = Vec::new();
    for value in [9, 10, 99, 100, u64::MAX] {
        let mut record = fact(1, value, &[], json!({"n":value}));
        if let RunRecordData::Chain(row) = &mut record.record {
            row.envelope.provenance.event.event_type =
                format!("sensor.{}.v1", "reading_".repeat(value.min(10) as usize));
        }
        renderer.record(&mut output, record).unwrap();
    }
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    let lines: Vec<_> = text.lines().collect();
    let clocks: Vec<_> = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            let clock = line.strip_prefix('⟨')?;
            assert!(
                lines[index - 1].ends_with("thermometer()"),
                "clock belongs below the complete output equation"
            );
            Some(format!("⟨{clock}"))
        })
        .collect();
    assert_eq!(
        clocks,
        [9, 10, 99, 100, u64::MAX].map(|sequence| displayed_clock(1, sequence))
    );
    assert_eq!(
        text.matches('⟨').count(),
        clocks.len(),
        "no inline or input clocks"
    );
}

#[test]
fn writer_highlight_uses_recorded_identity_and_row_color_instead_of_counter_size() {
    let mut renderer = renderer();
    renderer.color = true;
    let mut source = fact(1, 9, &[], json!({"celsius":38}));
    if let RunRecordData::Chain(row) = &mut source.record {
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(coordinate(2), 999);
    }
    let mut transform = fact(2, 10, &[9], json!({"status":"too_hot"}));
    if let RunRecordData::Chain(row) = &mut transform.record {
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(coordinate(1), 100);
    }
    let mut output = Vec::new();
    renderer.record(&mut output, source).unwrap();
    renderer.record(&mut output, transform).unwrap();
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.contains("\x1b[1;4;38;5;208m9\x1b[0m"),
        "source writer matches the fact color"
    );
    assert!(
        text.contains("\x1b[1;4;38;5;208m10\x1b[0m"),
        "transform writer matches the fact color"
    );
    assert!(
        text.contains("\x1b[38;5;245m999\x1b[0m"),
        "another writer's larger history stays gray"
    );
    assert!(
        text.contains("\x1b[38;5;245m100\x1b[0m"),
        "merged history stays gray"
    );
    assert!(text.contains(
        "\x1b[1;38;5;208mSOURCE\x1b[0m\n\x1b[1;38;5;208msensor.reading.v1 ← thermometer()\x1b[0m"
    ));
    assert!(
        text.contains("\x1b[1;38;5;208mTRANSFORM\x1b[0m\n\x1b[1;38;5;208msensor.classified.v1 ← classify(sensor.reading.v1)\x1b[0m")
    );
    for line in text.lines().filter(|line| !line.contains('⟨')) {
        assert!(
            line.matches('\x1b').count() <= 2,
            "only clocks may switch colors within a line: {line}"
        );
    }
    for underlined in text.split("\x1b[1;4;38;5;").skip(1) {
        let digits = underlined
            .split_once('m')
            .unwrap()
            .1
            .split_once("\x1b[0m")
            .unwrap()
            .0;
        assert!(
            digits.chars().all(|c| c.is_ascii_digit()),
            "underline only the counter's digits: {digits:?}"
        );
    }
}

#[test]
fn stateful_and_catalog_join_outputs_keep_recorded_inputs_with_green_styling() {
    for (stage_type, heading) in [(StageType::Stateful, "STATEFUL"), (StageType::Join, "JOIN")] {
        let mut renderer = renderer();
        renderer.color = true;
        let mut catalog = fact(1, 99, &[], json!({"offset":2}));
        if let RunRecordData::Chain(row) = &mut catalog.record {
            row.envelope.provenance.event.event_type = "sensor.calibration.v1".into();
        }
        let mut result = fact(2, 101, &[99, 100], json!({"celsius":40}));
        result.journal.stage.as_mut().unwrap().stage_type = stage_type;
        if let RunRecordData::Chain(row) = &mut result.record {
            row.envelope.provenance.event.flow_context.stage_type = stage_type;
        }
        let mut output = Vec::new();
        for record in [catalog, fact(1, 100, &[], json!({"celsius":38})), result] {
            renderer.record(&mut output, record).unwrap();
        }
        renderer.flush_pending(&mut output).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains(&format!(
            "\x1b[1;38;5;114m{heading}\x1b[0m\n\x1b[1;38;5;114msensor.classified.v1 ← classify(sensor.calibration.v1, sensor.reading.v1)\x1b[0m"
        )), "{text}");
        assert!(text.contains("\x1b[1;4;38;5;114m101\x1b[0m"));
        assert!(text.contains("  \"celsius\": 40"));
        assert!(!text.contains("state =") && !text.contains("state'"));
    }
}

#[test]
fn arbitrary_payload_shapes_keep_types_units_and_escape_terminal_controls() {
    let value = json!({
        "amount_cents":1234, "nested":{"label":"a\u{1b}[2J\nb\u{202e}"},
        "items":[1,2,3,4,5], "numeric_string":"1234", "enabled":true
    });
    let (text, shortened) = pretty(&value, 80);
    assert!(!shortened);
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), value);
    assert!(text.starts_with("{\n  \"amount_cents\": 1234,"));
    assert!(!text.contains('·') && !text.contains('\x1b') && !text.contains('\u{202e}'));
    assert!(text.lines().all(|line| line.chars().count() <= 80));
    assert_eq!(pretty(&json!(42), 80), ("42".into(), false));
    assert_eq!(pretty(&Value::Null, 80), ("null".into(), false));
    assert_eq!(pretty(&json!({}), 80), ("{}".into(), false));
    assert_eq!(pretty(&json!([]), 80), ("[]".into(), false));
}

#[test]
fn long_json_strings_fit_the_width_without_splitting_escapes_or_mutating_the_record() {
    let value = json!({"reason":{"nested":["é\"\\\n\u{202e}".repeat(30)]}});
    let original = value.clone();
    for width in [40, 60, 80, 90] {
        let (text, shortened) = pretty(&value, width);
        assert!(shortened);
        assert!(
            text.lines().all(|line| line.chars().count() <= width),
            "{text}"
        );
        let preview: Value = serde_json::from_str(&text).unwrap();
        let preview = preview["reason"]["nested"][0].as_str().unwrap();
        assert!(original["reason"]["nested"][0]
            .as_str()
            .unwrap()
            .starts_with(preview.strip_suffix('…').unwrap()));
        assert_eq!(value, original);
    }
}

#[test]
fn failed_processing_remains_distinct_from_a_successfully_produced_fact() {
    let mut renderer = renderer();
    let mut output = Vec::new();
    let mut failed = fact(2, 101, &[100], json!({"sensor":"A"}));
    failed.journal.kind = RunJournalKind::Error;
    if let RunRecordData::Chain(row) = &mut failed.record {
        row.envelope.provenance.event.processing.status =
            ProcessingStatus::error("calibration unavailable");
    }
    renderer
        .record(&mut output, fact(1, 100, &[], json!({"celsius":38})))
        .unwrap();
    renderer.record(&mut output, failed).unwrap();
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("[processing error]"));
    assert!(text.contains("processing error: calibration unavailable"));
    assert!(text.contains("\"sensor\": \"A\""));
}
