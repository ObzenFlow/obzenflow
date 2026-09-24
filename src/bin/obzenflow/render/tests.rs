// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use serde_json::json;

fn id(value: u64) -> String {
    format!("{value:026X}")
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
        "version": 1,
        "run": {"flow_id": id(10), "pipeline_writer_id": {"type":"System", "id":id(11)}},
        "journal": {"id":id(stage), "kind":"data", "stage":{"key":key, "id":id(stage), "stage_type":stage_type}},
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
                    "vector_clock":{"clocks":{format!("writer_stage_{}",id(stage)):event}},
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
        !text.contains("⟨0,102⟩"),
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
        renderer.verbose = verbose;
        let mut output = Vec::new();
        for record in [
            fact(1, 100, &[], json!({"celsius":38})),
            failed.clone(),
            progress.clone(),
        ] {
            renderer.record(&mut output, record).unwrap();
        }
        renderer.flush_pending(&mut output).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("\x1b[38;5;217mEFFECT\x1b[0m\n\x1b[38;5;217mobzenflow.effect_record.v1 ← classify(sensor.reading.v1)\x1b[0m"));
        assert!(text.contains("outcome: failed") && text.contains("effect_type: sensor.calibrate"));
        assert!(text.contains("error_message: calibration unavailable"));
        assert_eq!(text.contains("RUNTIME"), verbose);
        if verbose {
            let runtime = text.split("\x1b[38;5;245mRUNTIME").nth(1).unwrap();
            assert!(runtime.contains("\x1b[38;5;245mexecution_type:"));
            assert!(!runtime.contains("38;5;208m") && !runtime.contains("38;5;217m"));
        }
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
        text.contains("SOURCE\nsensor.reading.v1 ← thermometer()\n⟨100,0⟩"),
        "{text}"
    );
    assert!(
        text.contains("TRANSFORM\nsensor.classified.v1 ← classify(sensor.reading.v1)\n⟨0,101⟩"),
        "{text}"
    );
    assert!(
        text.contains(
            "TRANSFORM\nsensor.cooling_requested.v1 ← classify(sensor.reading.v1)\n⟨0,102⟩"
        ),
        "{text}"
    );
    let clocks: Vec<_> = text
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with('⟨'))
        .collect();
    assert_eq!(clocks, ["⟨100,0⟩", "⟨0,101⟩", "⟨0,102⟩"]);
    let last_clock = text.find("⟨0,102⟩").unwrap();
    let last_fact = text
        .find("sensor.cooling_requested.v1 ← classify(sensor.reading.v1)")
        .unwrap();
    assert!(
        last_fact < last_clock,
        "each grouped fact retains its own clock below its typed equation"
    );
    assert_eq!(
        text.matches("sensor: A").count(),
        2,
        "each fact keeps its own payload, even when fields match: {text}"
    );
    for (event_type, reason) in [
        ("sensor.classified.v1", "reason: too_hot"),
        (
            "sensor.cooling_requested.v1",
            "reason: {cooling: requested}",
        ),
    ] {
        let block = text
            .split("\n\n")
            .find(|block| block.starts_with(&format!("TRANSFORM\n{event_type} ← ")))
            .unwrap();
        assert!(
            block.contains("sensor: A") && block.contains(reason),
            "{block}"
        );
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
fn clocks_keep_unknown_writers_and_do_not_derive_causality_from_dominance() {
    let mut renderer = renderer();
    let mut source = fact(1, 100, &[], json!({}));
    if let RunRecordData::Chain(row) = &mut source.record {
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert("another_writer".into(), 12);
    }
    let mut output = Vec::new();
    renderer.record(&mut output, source).unwrap();
    renderer
        .record(&mut output, fact(2, 101, &[], json!({})))
        .unwrap();
    renderer.flush_pending(&mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("⟨100,0,another_writer:12⟩"), "{text}");
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
        [
            "⟨9,0⟩",
            "⟨10,0⟩",
            "⟨99,0⟩",
            "⟨100,0⟩",
            "⟨18446744073709551615,0⟩"
        ]
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
            .insert(format!("writer_stage_{}", id(2)), 999);
    }
    let mut transform = fact(2, 10, &[9], json!({"status":"too_hot"}));
    if let RunRecordData::Chain(row) = &mut transform.record {
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(format!("writer_stage_{}", id(1)), 100);
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
        assert!(text.contains("  celsius: 40"));
        assert!(!text.contains("state =") && !text.contains("state'"));
    }
}

#[test]
fn arbitrary_payload_shapes_keep_types_units_and_escape_terminal_controls() {
    let parts = fields(&json!({
        "amount_cents":1234, "nested":{"label":"a\u{1b}[2J\nb"},
        "items":[1,2,3,4,5], "numeric_string":"1234", "enabled":true
    }));
    let text = wrap_fields(&parts, 60).join("\n");
    assert!(text.contains("amount_cents: 1234") && !text.contains('$'));
    assert!(text.contains("numeric_string: \"1234\""));
    assert!(text.contains("… 1 more") && text.contains("enabled: true"));
    assert!(!text.contains('\x1b') && text.contains("\\u{1b}[2J\\nb"));
    assert_eq!(fields(&json!(42)), ["42"]);
    assert_eq!(fields(&Value::Null), ["null"]);
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
    assert!(text.contains("sensor: A"));
}
