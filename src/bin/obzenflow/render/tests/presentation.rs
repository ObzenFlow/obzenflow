// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::types::{DurationMs, JournalIndex, JournalPath, SeqNo};
use obzenflow_core::event::vector_clock::VectorClock;

fn progress() -> RunRecord {
    let mut record = fact(2, 101, &[], json!({}));
    record.kind = RunRecordKind::FlowSignal;
    if let RunRecordData::Chain(row) = &mut record.record {
        row.payload = ChainPayload::FlowControl(FlowControlPayload::ConsumptionProgress {
            reader_seq: SeqNo(2),
            last_event_id: Some(serde_json::from_value(json!(id(100))).unwrap()),
            vector_clock: Some(watermark(7)),
            eof_seen: false,
            reader_path: JournalPath(format!("stage_{}", id(31))),
            reader_index: JournalIndex(99),
            advertised_writer_seq: None,
            advertised_vector_clock: Some(watermark(7)),
            stalled_since: None,
        });
        row.envelope.provenance.event.event_kind = row.payload.kind();
        row.envelope.provenance.event.event_type =
            row.payload.framework_event_type().unwrap().into();
    }
    record
}

fn watermark(sequence: u64) -> VectorClock {
    VectorClock {
        clocks: BTreeMap::from([(coordinate(1), sequence), (coordinate(9), 12)]),
    }
}

fn metrics_export() -> RunRecord {
    use obzenflow_core::event::{MetricsCoordinationEvent, SystemEvent};
    let mut value = serde_json::to_value(fact(3, 119, &[], json!({}))).unwrap();
    let authored = SystemEvent::new(
        obzenflow_core::SystemId::new().into(),
        SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Exported {
            watermark: watermark(7),
        }),
    );
    value["journal"]["kind"] = json!("metrics_export");
    value["journal"]["stage"] = Value::Null;
    value["kind"] = json!("system");
    value["record"]["envelope"]["provenance"]["event"] =
        serde_json::to_value(authored.envelope.provenance.event).unwrap();
    value["record"]["payload"] = serde_json::to_value(authored.payload).unwrap();
    serde_json::from_value(value).unwrap()
}

fn progress_payload(record: &mut RunRecord) -> &mut FlowControlPayload {
    let RunRecordData::Chain(row) = &mut record.record else {
        unreachable!()
    };
    let ChainPayload::FlowControl(payload) = &mut row.payload else {
        unreachable!()
    };
    payload
}

fn progress_renderer() -> Renderer {
    let mut source = fact(1, 100, &[], json!({})).journal;
    // Physical journal ID, stage ID and subscription-local reader index are
    // deliberately different. Only metadata establishes this association.
    source.stage.as_mut().unwrap().id = id(31).parse().unwrap();
    let mut renderer = Renderer::new(
        &ViewArgs {
            include_runtime: true,
            ..ViewArgs::default()
        },
        false,
        false,
        [&source, &progress().journal].into_iter(),
    );
    renderer.width = 90;
    renderer
}

fn render_record(renderer: &mut Renderer, record: &RunRecord) -> String {
    let mut output = Vec::new();
    renderer.record(&mut output, record.clone()).unwrap();
    renderer.flush_pending(&mut output).unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn consumption_progress_resolves_upstream_and_keeps_only_recorded_status() {
    let record = progress();
    let original = serde_json::to_value(&record).unwrap();
    let mut renderer = progress_renderer();
    let text = render_record(&mut renderer, &record);
    assert!(text.contains("RUNTIME (stage: classify, journal: 2)\ncontrol.consumption_progress ← classify\n⟨2:101⟩\nInput: thermometer (journal: 1)\nProgress: 2 · EOF: not seen\n"), "{text}");
    assert!(!text.contains("Advertised:") && !text.contains("Stalled:"));
    assert!(!text.contains("last_event_id") && !text.contains("journal_writer_id"));
    assert!(!text.contains("Input watermark") && !text.contains("caught up"));
    assert_eq!(text.matches('⟨').count(), 1);
    assert_eq!(serde_json::to_value(&record).unwrap(), original);

    let mut record = progress();
    if let FlowControlPayload::ConsumptionProgress {
        reader_seq,
        eof_seen,
        advertised_writer_seq,
        stalled_since,
        ..
    } = progress_payload(&mut record)
    {
        *reader_seq = SeqNo(u64::MAX);
        *eof_seen = true;
        *advertised_writer_seq = Some(SeqNo(0));
        *stalled_since = Some(DurationMs(u64::MAX));
    }
    renderer.width = 40;
    let text = render_record(&mut renderer, &record);
    let joined = text.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        joined.contains(&format!(
            "Progress: {} · EOF: seen · Advertised: 0 · Stalled: {} ms",
            u64::MAX,
            u64::MAX
        )),
        "{text}"
    );
    assert!(
        text.lines().all(|line| line.chars().count() <= 40),
        "{text}"
    );
}

#[test]
fn explanatory_watermarks_share_aliases_but_never_claim_catchup_or_gain_an_underline() {
    for equal in [false, true] {
        let mut record = progress();
        if !equal {
            if let FlowControlPayload::ConsumptionProgress {
                advertised_vector_clock,
                ..
            } = progress_payload(&mut record)
            {
                *advertised_vector_clock = Some(watermark(8));
            }
        }
        let mut renderer = progress_renderer();
        renderer.explain = true;
        let text = render_record(&mut renderer, &record);
        assert!(
            text.contains("Input watermark (recorded):\n⟨1:7,3:12⟩"),
            "{text}"
        );
        assert!(text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .contains("do not establish that the reader has caught up"));
        if equal {
            assert!(text.contains("Advertised clock: same recorded clock as the input watermark."));
            assert_eq!(text.matches("⟨1:7,3:12⟩").count(), 1);
        } else {
            assert!(text.contains("Advertised clock (recorded):\n⟨1:8,3:12⟩"));
        }
        renderer.color = true;
        let colored = render_record(&mut renderer, &record);
        assert_eq!(colored.matches("\x1b[1;4;38;5;").count(), 1);
        assert!(colored.contains("\x1b[1;4;38;5;250m101\x1b[0m"));
    }
}

#[test]
fn missing_and_ambiguous_upstream_metadata_never_invents_a_journal_alias() {
    let mut record = progress();
    if let FlowControlPayload::ConsumptionProgress { reader_path, .. } =
        progress_payload(&mut record)
    {
        *reader_path = JournalPath("unresolved\u{1b}[2J\n\u{202e}".into());
    }
    let text = render_record(&mut progress_renderer(), &record);
    assert!(text.contains("reader index: 99"));
    assert!(!text.contains('\x1b') && !text.contains('\u{202e}'));
    assert!(!text.contains("Input: thermometer"));
    let mut compact = progress_renderer();
    compact.compact = true;
    compact.width = 500;
    let text = render_record(&mut compact, &record);
    assert_eq!(text.lines().count(), 1);
    assert!(text.contains("unresolved\\u{1b}[2J\\n\\u{202e}"), "{text}");

    let mut renderer = progress_renderer();
    let mut incarnation = fact(3, 102, &[], json!({}));
    let stage = incarnation.journal.stage.as_mut().unwrap();
    stage.id = id(31).parse().unwrap();
    stage.key = "thermometer".into();
    renderer.context.remember(&incarnation);
    let text = render_record(&mut renderer, &progress());
    assert!(text.contains("Input: thermometer\n"), "{text}");
    assert!(!text.contains("Input: thermometer (journal:"));
}

#[test]
fn hidden_payload_clocks_register_numbers_before_any_presentation_mode_uses_them() {
    let record = progress();
    for include_runtime in [false, true] {
        for explain in [false, true] {
            let mut renderer = progress_renderer();
            renderer.include_runtime = include_runtime;
            renderer.explain = explain;
            render_record(&mut renderer, &record);
            assert_eq!(
                renderer
                    .context
                    .journal_number(coordinate(9).journal_writer_id.as_journal_id()),
                3
            );
            let mut later = fact(9, 102, &[], json!({}));
            later.journal.stage.as_mut().unwrap().key = "late_upstream".into();
            let text = render_record(&mut renderer, &later);
            assert!(text.contains("3 late_upstream"), "{text}");
            assert!(text.contains("⟨3:102⟩"), "{text}");
        }
    }
}

#[test]
fn metrics_exports_keep_the_event_clock_and_only_explain_the_watermark_on_request() {
    let record = metrics_export();
    let mut renderer = progress_renderer();
    let text = render_record(&mut renderer, &record);
    assert!(text.contains("system.metrics.exported ←"));
    assert!(text.contains("⟨3:119⟩"));
    assert!(!text.contains("watermark") && !text.contains("metrics_event"));
    renderer.explain = true;
    let text = render_record(&mut renderer, &record);
    assert!(
        text.contains("Export watermark (recorded):\n⟨1:7,4:12⟩"),
        "{text}"
    );
    assert!(text.contains("⟨3:119⟩"));
}

#[test]
fn full_output_prints_the_unchanged_source_once_without_projected_payload_duplication() {
    for record in [
        progress(),
        metrics_export(),
        fact(
            1,
            100,
            &[],
            json!({"long": "é\n\u{1b}[2J\u{202e}".repeat(50)}),
        ),
    ] {
        let mut renderer = progress_renderer();
        renderer.full = true;
        let text = render_record(&mut renderer, &record);
        assert_eq!(text.matches("\"envelope\":").count(), 1, "{text}");
        assert!(!text.contains("Input: thermometer"));
        let source = text.split_once("\n{\n").unwrap().1;
        let source = format!("{{\n{source}");
        assert_eq!(
            serde_json::from_str::<Value>(&source).unwrap(),
            serde_json::to_value(record).unwrap()
        );
        assert!(!text.contains('\x1b') && !text.contains('\u{202e}'));
    }
}

#[test]
fn jsonl_uses_the_common_view_without_requiring_human_context_or_parent_resolution() {
    for record in [
        progress(),
        metrics_export(),
        fact(2, 101, &[9999], json!({"long": "x".repeat(500)})),
    ] {
        let view = ViewArgs {
            jsonl: true,
            include_runtime: true,
            color: ColorMode::Always,
            ..ViewArgs::default()
        };
        // No journal inventory or resolved parents: JSONL must still be exact
        // and immediate, with neither a legend nor formatting prerequisites.
        let mut renderer = Renderer::new(&view, true, false, std::iter::empty());
        let mut output = Vec::new();
        renderer.record(&mut output, record.clone()).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert_eq!(
            text,
            format!("{}\n", serde_json::to_string(&record).unwrap())
        );
        assert!(renderer.pending.is_empty());
    }
}

#[test]
fn application_payloads_never_trigger_special_treatments_and_preserve_all_values() {
    let payload = json!({
        "flow_control_type": "consumption_progress",
        "metrics_event": "exported",
        "watermark": {"entries": [{"journal_writer_id": "not a journal", "sequence": u64::MAX}]},
        "vector_clock": null,
        "reader_seq": "002",
        "nested": [true, 0, 1.25, "é\n\u{202e}\u{1b}[2J".repeat(100)]
    });
    let record = fact(1, 100, &[], payload.clone());
    let mut renderer = renderer();
    renderer.width = 40;
    let text = render_record(&mut renderer, &record);
    let json = format!("{{\n{}", text.split_once("\n{\n").unwrap().1);
    assert_eq!(serde_json::from_str::<Value>(&json).unwrap(), payload);
    assert!(!text.contains("Input:") && !text.contains("Payload shortened"));
    assert!(!text.contains('\x1b') && !text.contains('\u{202e}'));
}
