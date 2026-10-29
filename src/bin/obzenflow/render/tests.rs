// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::context::{event_id, event_type, writer_id};
use super::payload::{abbreviated, compact};
use super::*;
use obzenflow::journal::ProcessingStatus;
use serde_json::{json, Value};

#[path = "tests/presentation.rs"]
mod presentation;

fn id(value: u64) -> String {
    format!("{value:026X}")
}

fn coordinate(stage: u64) -> obzenflow_core::event::CausalCoordinate {
    serde_json::from_value(json!({"journal_writer_id":id(stage)})).unwrap()
}

fn displayed_clock(stage: u64, sequence: u64) -> String {
    format!("⟨{stage}:{sequence}⟩")
}

fn record_clock(renderer: &Renderer, record: &RunRecord) -> String {
    let view = EventView::from_record(record, &renderer.context);
    renderer.terminal().clock(&view.clock(), &view.header())
}

fn fact(stage: u64, event: u64, parents: &[u64], payload: Value) -> RunRecord {
    let (key, stage_type, kind, event_type) = if stage == 1 {
        (
            "thermometer",
            "FiniteSource",
            "source_fact",
            "sensor.reading",
        )
    } else {
        ("classify", "Transform", "stage_output", "sensor.classified")
    };
    serde_json::from_value(json!({
        "version": 3,
        "run": {"flow_id": id(10), "pipeline_writer_id": {"type":"System", "id":id(11)}},
        "journal": {"id":id(stage), "kind":"data", "stage":{"key":key, "id":id(stage), "stage_type":stage_type,"is_effectful":false}},
        "position":event,
        "kind":kind,
        "record": {
            "envelope": {"provenance": {
                "event": {
                    "id":id(event), "writer_id":{"type":"Stage", "id":id(stage)},
                    "event_kind":"fact", "event_type":event_type, "payload_schema_version":1,
                    "causality":{
                        "parent_ids":parents.iter().map(|p| id(*p)).collect::<Vec<_>>(),
                        "inputs":parents.iter().map(|p| json!({"id":id(*p), "event_type":"sensor.reading"})).collect::<Vec<_>>()
                    },
                    "flow_context":{"flow_name":"sensors", "flow_id":id(10), "stage_name":key, "stage_id":id(stage), "stage_type":stage_type},
                    "processing":{"event_time":0, "status":"Success"}, "composite_activations":[]
                },
                "journal": {
                    "journal_writer_id":id(stage),
                    "run_id":id(10),
                    "previous":{"run_id":id(10),"journal_writer_id":id(stage),"sequence":event-1,"event_id":id(event-1)},
                    "vector_clock":{"entries":[{"journal_writer_id":id(stage),"sequence":event}]},
                    "timestamp":"2026-09-23T00:00:00Z", "journal_group_id":null, "journal_group_member":null
                }
            }},
            "payload":payload
        }
    })).unwrap()
}

fn with_inputs(mut record: RunRecord, inputs: &[(u64, &str)]) -> RunRecord {
    if let RunRecordData::Chain(row) = &mut record.record {
        row.envelope.provenance.event.causality.inputs = serde_json::from_value(json!(inputs
            .iter()
            .map(|(event, event_type)| json!({"id": id(*event), "event_type": event_type}))
            .collect::<Vec<_>>()))
        .unwrap();
    }
    record
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
    renderer.begin(&mut Vec::new(), &source.run, false).unwrap();
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
            journal.event_types[&(
                context::event_descriptor(record),
                context::record_writer_id(record)
            )],
            expected
        );
        assert_eq!(journal.omitted, 0);
    }
}

#[test]
fn event_counts_distinguish_payload_versions_with_identical_names_and_bodies() {
    let first = fact(1, 100, &[], json!({"value": 1}));
    let mut second = first.clone();
    let RunRecordData::Chain(row) = &mut second.record else {
        unreachable!()
    };
    row.envelope.provenance.event.payload_schema_version = std::num::NonZeroU32::new(2).unwrap();
    let mut counts = event_counts::EventCounts::default();
    counts.record(&first);
    counts.record(&second);
    let journal = counts.journals().next().unwrap();
    assert_eq!(
        journal.event_types[&(
            context::event_descriptor(&first),
            context::record_writer_id(&first)
        )],
        1
    );
    assert_eq!(
        journal.event_types[&(
            context::event_descriptor(&second),
            context::record_writer_id(&second)
        )],
        1
    );
}

#[test]
fn journal_summary_preserves_distinct_authors_punctuation_and_lexical_versions() {
    use obzenflow_core::event::payloads::supervisor_descriptor::{
        SupervisionMode, SupervisorDescriptor, SupervisorKind,
    };

    let source = fact(1, 100, &[], json!({}));
    let second_author = fact(2, 101, &[], json!({}));
    let stage = source.journal.stage.as_ref().unwrap();
    let manifest: RunManifest = serde_json::from_value(json!({
        "journal_schema_version": obzenflow_core::journal::JOURNAL_SCHEMA_VERSION,
        "obzenflow_version": "test", "flow_id": id(10),
        "pipeline_writer_id": source.run.pipeline_writer_id,
        "flow_name": "sensors", "created_at": "2026-09-23T00:00:00Z",
        "system_journal_file": "system.log", "metrics_journals": null,
        "stages": {stage.key.clone(): {
            "dsl_var": stage.key, "stage_type": stage.stage_type, "is_effectful": false,
            "stage_id": stage.id.to_string(), "stage_logic_version": "1",
            "data_journal_file": "thermometer.log", "error_journal_file": "thermometer.errors.log",
            "inbound": [], "delivery_order": "single_input"
        }}
    }))
    .unwrap();
    let mut renderer = Renderer::new(
        &ViewArgs::default(),
        false,
        false,
        [&source.journal].into_iter(),
    );
    renderer.width = 90;
    renderer.context.stages.clear();
    for record in [&source, &second_author] {
        renderer.context.supervisors.insert(
            writer_id(record),
            SupervisorDescriptor {
                name: "shared".into(),
                kind: SupervisorKind::Transform,
                supervision: SupervisionMode::HandlerSupervised,
            },
        );
    }
    // These names deliberately contain the punctuation of descriptor Display.
    for (author, name, version) in [
        (&source, "sensor.value@unit/reading", 2),
        (&source, "sensor.value@unit/reading", 10),
        (&source, "sensor.value@unit/reading", 1),
        (&source, "sensor.value", 1),
        (&source, "runtime.stream.watermark_declared", 1),
        (&second_author, "sensor.value@unit/reading", 1),
    ] {
        let mut record = author.clone();
        record.journal = source.journal.clone();
        let RunRecordData::Chain(row) = &mut record.record else {
            unreachable!()
        };
        row.envelope.provenance.event.event_type = name.into();
        row.envelope.provenance.event.payload_schema_version =
            std::num::NonZeroU32::new(version).unwrap();
        renderer.event_counts.record(&record);
    }
    let mut output = Vec::new();
    renderer
        .finish(
            &mut output,
            &mut Vec::new(),
            Observed {
                run: &source.run,
                manifest: &manifest,
                progress: &RunReadProgress::default(),
                lineage: Ok(RunLineage::default()),
                config_changes: Ok(Vec::new()),
            },
            ObservationEnd::Snapshot,
        )
        .unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.lines().all(|line| line.chars().count() <= 90),
        "{text}"
    );
    assert!(!text.contains("Author:") && !text.contains("Event prefix:"));
    assert_eq!(
        text.lines()
            .filter(|line| line.trim_start().starts_with("Count "))
            .count(),
        1
    );
    let summary = text.split_once("\nAPPLICATION STAGES\n").unwrap().1;
    let rows: Vec<_> = summary
        .lines()
        .filter_map(|line| {
            let cells: Vec<_> = line.split_whitespace().collect();
            (cells.len() == 3 && cells[0] == "1").then_some(cells)
        })
        .collect();
    assert_eq!(
        rows,
        [
            vec!["1", "fact/runtime.stream.watermark_declared@1", "shared"],
            vec!["1", "fact/sensor.value@1", "shared"],
            vec!["1", "fact/sensor.value@unit/reading@1", "shared"],
            vec!["1", "fact/sensor.value@unit/reading@10", "shared"],
            vec!["1", "fact/sensor.value@unit/reading@2", "shared"],
            vec!["1", "fact/sensor.value@unit/reading@1", "shared"],
        ]
    );
}

#[test]
fn journal_summary_groups_data_and_errors_by_owner_not_forwarded_author() {
    let source = fact(1, 100, &[], json!({}));
    let mut forwarded = source.clone();
    forwarded.journal = fact(2, 101, &[100], json!({})).journal;
    let mut error = forwarded.clone();
    error.journal.id = serde_json::from_value(json!(id(3))).unwrap();
    error.journal.kind = RunJournalKind::Error;
    let stages: serde_json::Map<String, Value> = [&source, &forwarded]
        .into_iter()
        .map(|record| {
            let stage = record.journal.stage.as_ref().unwrap();
            (
                stage.key.clone(),
                json!({
                    "dsl_var": stage.key,
                    "stage_type": stage.stage_type,
                    "is_effectful": false,
                    "stage_id": stage.id.to_string(),
                    "stage_logic_version": "1",
                    "data_journal_file": format!("{}.log", stage.key),
                    "error_journal_file": format!("{}.errors.log", stage.key),
                    "inbound": if stage.key == "classify" { vec!["thermometer"] } else { vec![] },
                    "delivery_order": "single_input"
                }),
            )
        })
        .collect();
    let manifest: RunManifest = serde_json::from_value(json!({
        "journal_schema_version": obzenflow_core::journal::JOURNAL_SCHEMA_VERSION,
        "obzenflow_version": "test",
        "flow_id": id(10),
        "pipeline_writer_id": source.run.pipeline_writer_id,
        "flow_name": "sensors",
        "created_at": "2026-09-23T00:00:00Z",
        "stages": stages,
        "system_journal_file": "system.log",
        "metrics_journals": null
    }))
    .unwrap();
    for width in [40, 90] {
        let records = [&source, &forwarded, &error];
        let mut renderer = Renderer::new(
            &ViewArgs::default(),
            false,
            false,
            records.iter().map(|record| &record.journal),
        );
        renderer.width = width;
        // Count the forwarded evidence in each physical history. Its source
        // author must not move either journal beneath the source stage.
        for record in records {
            renderer.event_counts.record(record);
        }
        let mut output = Vec::new();
        renderer
            .finish(
                &mut output,
                &mut Vec::new(),
                Observed {
                    run: &source.run,
                    manifest: &manifest,
                    progress: &RunReadProgress::default(),
                    lineage: Ok(RunLineage::default()),
                    config_changes: Ok(Vec::new()),
                },
                ObservationEnd::Snapshot,
            )
            .unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(
            text.lines().all(|line| line.chars().count() <= width),
            "{text}"
        );
        let groups = text.split_once("\nAPPLICATION STAGES\n").unwrap().1;
        let (source_group, transform_group) = groups.split_once("\nTRANSFORM: classify\n").unwrap();
        assert_eq!(groups.matches("\nSOURCE: thermometer\n").count(), 1);
        assert_eq!(groups.matches("\nTRANSFORM: classify\n").count(), 1);
        assert_eq!(groups.matches("  Owns:\n").count(), 2);
        assert!(source_group.contains("\n    thermometer.log\n"));
        assert!(!source_group.contains("classify.log"));
        assert!(transform_group.contains("\n    classify.log\n"));
        assert!(transform_group.contains("\n    classify.errors.log\n"));
        assert_eq!(
            transform_group
                .lines()
                .filter(|line| line.trim_start().starts_with("Count "))
                .count(),
            2
        );
        let error_table = transform_group
            .split_once("\n    classify.errors.log\n")
            .unwrap()
            .1;
        assert!(
            error_table.contains("Runtime readers: —"),
            "pipeline does not consume error journals"
        );
        if width == 90 {
            assert_eq!(transform_group.matches("fact/sensor.reading@1").count(), 2);
            assert_eq!(
                transform_group
                    .lines()
                    .filter(|line| {
                        line.contains("fact/sensor.reading@1")
                            && line.trim_end().ends_with("thermometer")
                    })
                    .count(),
                2
            );
        }
    }
}

fn matrix_text(renderer: &Renderer, progress: &RunReadProgress) -> String {
    let mut output = Vec::new();
    renderer.clock_summary(&mut output, progress).unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn clock_matrix_tracks_hidden_and_forwarded_records_by_journal_position() {
    let source = fact(1, 10, &[], json!({}));
    let transform = fact(2, 11, &[10], json!({}));
    let mut empty = fact(3, 12, &[], json!({})).journal;
    empty.stage.as_mut().unwrap().key = "empty_stage".into();
    let mut error = empty.clone();
    error.id = serde_json::from_value(json!(id(4))).unwrap();
    error.kind = RunJournalKind::Error;
    let mut renderer = Renderer::new(
        &ViewArgs::default(),
        false,
        false,
        [&source.journal, &transform.journal, &empty, &error].into_iter(),
    );
    let mut output = Vec::new();
    for record in [source, transform.clone(), fact(1, 13, &[], json!({}))] {
        renderer.record(&mut output, record).unwrap();
    }
    let mut forwarded = fact(1, 13, &[], json!({}));
    forwarded.journal = transform.journal.clone();
    forwarded.position = JournalPosition(14);
    if let RunRecordData::Chain(row) = &mut forwarded.record {
        row.envelope.provenance.journal.journal_writer_id = forwarded.journal.id.into();
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(coordinate(2), 14);
    }
    renderer.record(&mut output, forwarded).unwrap();
    assert_eq!(
        renderer.context.journals[&transform.journal.id]
            .last_clock
            .as_ref()
            .unwrap()
            .position,
        JournalPosition(14)
    );

    let mut hidden = execution(
        15,
        &[13],
        ExecutionPayload::AccumulatorProgress {
            inputs_since_last_report: 1,
        },
    );
    if let RunRecordData::Chain(row) = &mut hidden.record {
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(coordinate(1), 13);
    }
    renderer.record(&mut output, hidden).unwrap();
    renderer.context.remember(&transform); // Revisiting an older record cannot replace the tail.
    let text = matrix_text(&renderer, &RunReadProgress::default());
    let rows: Vec<Vec<_>> = text
        .lines()
        .filter_map(|line| {
            let cells: Vec<_> = line.split_whitespace().collect();
            cells.first()?.parse::<usize>().ok()?;
            Some(cells)
        })
        .collect();
    assert_eq!(
        rows,
        [
            vec!["1", "thermometer", "13", "0", "0"],
            vec!["2", "classify", "13", "15", "0"],
            vec!["3", "empty_stage", "—", "—", "—"],
        ]
    );
    assert!(
        !text.contains("/error"),
        "empty error journals need no matrix row"
    );
    renderer.color = true;
    let colored = matrix_text(&renderer, &RunReadProgress::default());
    assert_eq!(colored.matches("\x1b[1;4;38;5;252m").count(), 2);
    assert!(colored.contains("\x1b[1;4;38;5;252m15\x1b[0m"));
    assert!(
        colored.contains("\x1b[38;5;252m13\x1b[0m"),
        "inherited history is not underlined"
    );
}

#[test]
fn clock_matrix_requires_settlement_and_preserves_wide_counters_and_external_histories() {
    let mut renderer = renderer();
    renderer.width = 40;
    let mut record = fact(1, u64::MAX, &[], json!({}));
    if let RunRecordData::Chain(row) = &mut record.record {
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(coordinate(9), 7);
    }
    renderer.context.remember(&record);
    let event_id = serde_json::from_value(json!(id(100))).unwrap();
    let mut progress = RunReadProgress {
        outcome: Some(RecordedRunOutcome {
            event_id,
            outcome: RunOutcome::Completed,
        }),
        drained_event_id: Some(event_id),
        ..Default::default()
    };
    let observed = matrix_text(&renderer, &progress);
    assert!(observed.contains("LAST OBSERVED JOURNAL CLOCKS"));
    assert!(observed.contains(&u64::MAX.to_string()));
    assert!(
        observed.lines().all(|line| line.chars().count() <= 40),
        "{observed}"
    );
    renderer.width = 160;
    let wide = matrix_text(&renderer, &progress);
    let external = coordinate(9).journal_writer_id.as_journal_id().to_string();
    assert!(wide
        .lines()
        .any(|line| line.split_whitespace().collect::<Vec<_>>()
            == ["3", external.as_str(), "—", "—", "—"]));
    assert!(wide
        .lines()
        .any(|line| line.split_whitespace().collect::<Vec<_>>()
            == ["1", "thermometer", &u64::MAX.to_string(), "0", "7"]));
    progress.settled_prefix = Some(SettledRunPrefix {
        terminal_event_id: event_id,
        drained_event_id: event_id,
        end_positions: BTreeMap::new(),
    });
    assert!(matrix_text(&renderer, &progress).contains("\nFINAL JOURNAL CLOCKS\n"));
}

#[test]
fn large_clock_matrices_are_explicit_and_split_into_complete_column_panels() {
    let journals: Vec<_> = (1..=20)
        .map(|index| {
            let mut journal = fact(index, 100, &[], json!({})).journal;
            journal.stage.as_mut().unwrap().key = format!("stage_{index:02}");
            journal
        })
        .collect();
    let mut renderer = Renderer::new(&ViewArgs::default(), false, false, journals.iter());
    renderer.width = 40;
    let summary = matrix_text(&renderer, &RunReadProgress::default());
    assert!(summary.contains("20 journals; use --full"));
    assert!(!summary.contains("stage_01"));
    renderer.full = true;
    let complete = matrix_text(&renderer, &RunReadProgress::default());
    let columns: Vec<_> = complete
        .lines()
        .filter(|line| line.trim_start().starts_with("#  Journal"))
        .flat_map(|line| {
            line.split_whitespace()
                .skip(2)
                .map(|column| column.parse::<usize>().unwrap())
        })
        .collect();
    assert_eq!(columns, (1..=20).collect::<Vec<_>>());
    assert!(
        complete.lines().all(|line| line.chars().count() <= 40),
        "{complete}"
    );
}

#[test]
fn journal_numbers_stay_fixed_across_hidden_records_late_journals_and_matrix_filtering() {
    let source = fact(1, 100, &[], json!({}));
    let transform = fact(2, 101, &[], json!({}));
    let mut error = source.journal.clone();
    error.id = *coordinate(4).journal_writer_id.as_journal_id();
    error.kind = RunJournalKind::Error;
    let mut coordination = source.journal.clone();
    coordination.id = *coordinate(5).journal_writer_id.as_journal_id();
    coordination.kind = RunJournalKind::MetricsCoordination;
    coordination.stage = None;
    let mut export = coordination.clone();
    export.id = *coordinate(6).journal_writer_id.as_journal_id();
    export.kind = RunJournalKind::MetricsExport;
    let inventory = [
        &error,
        &transform.journal,
        &export,
        &source.journal,
        &coordination,
    ];
    let mut renderer = Renderer::new(&ViewArgs::default(), false, false, inventory.into_iter());
    renderer.width = 90;
    let mut reversed = Context::new(inventory.into_iter().rev());
    let expected = vec![
        (1, "metrics/coordination".into()),
        (2, "metrics/export".into()),
        (3, "thermometer".into()),
        (4, "classify".into()),
        (5, "thermometer/error".into()),
    ];
    assert_eq!(reversed.take_journal_labels(), expected);
    let original: Vec<_> = inventory
        .iter()
        .map(|journal| (journal.id, renderer.context.journal_number(&journal.id)))
        .collect();
    let mut output = Vec::new();
    renderer.begin(&mut output, &source.run, true).unwrap();
    let legend = String::from_utf8(std::mem::take(&mut output)).unwrap();
    for (number, name) in expected {
        assert!(legend.contains(&format!("{number} {name}")), "{legend}");
    }

    let mut hidden = execution(
        101,
        &[],
        ExecutionPayload::AccumulatorProgress {
            inputs_since_last_report: 1,
        },
    );
    if let RunRecordData::Chain(row) = &mut hidden.record {
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(coordinate(9), 7);
    }
    renderer.record(&mut output, hidden).unwrap();
    let update = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(update.contains(&format!("6 journal_{}", id(9))));
    assert!(
        !update.contains("RUNTIME"),
        "hidden records still extend the legend"
    );

    // A new journal that would sort earlier gets appended. An earlier unknown
    // reference keeps its number when its own record supplies its name.
    for (journal, event, name) in [(8, 102, "a_new_stage"), (9, 103, "late_stage")] {
        let mut record = fact(journal, event, &[], json!({}));
        record.journal.stage.as_mut().unwrap().key = name.into();
        renderer.record(&mut output, record).unwrap();
    }
    let updates = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(updates.contains("7 a_new_stage"));
    assert!(updates.contains("6 late_stage"));
    assert!(updates.contains("TRANSFORM (stage: late_stage, journal: 6)"));
    assert!(updates.contains("⟨6:103⟩"));
    let matrix = matrix_text(&renderer, &RunReadProgress::default());
    let columns: Vec<_> = matrix
        .lines()
        .find(|line| line.trim_start().starts_with("#  Journal"))
        .unwrap()
        .split_whitespace()
        .skip(2)
        .collect();
    assert_eq!(columns, ["1", "2", "3", "4", "6", "7"]);
    assert!(!matrix.contains("thermometer/error"));

    let mut forwarded = source.clone();
    forwarded.journal = error;
    if let RunRecordData::Chain(row) = &mut forwarded.record {
        row.envelope.provenance.journal.journal_writer_id = forwarded.journal.id.into();
        row.envelope
            .provenance
            .journal
            .vector_clock
            .clocks
            .insert(coordinate(4), 1);
    }
    renderer.record(&mut output, forwarded.clone()).unwrap();
    assert_eq!(record_clock(&renderer, &forwarded), "⟨3:100,5:1⟩");
    let matrix = matrix_text(&renderer, &RunReadProgress::default());
    assert!(matrix.lines().any(|line| {
        let mut cells = line.split_whitespace();
        cells.next() == Some("5") && cells.next() == Some("thermometer/error")
    }));
    for (id, number) in original {
        assert_eq!(renderer.context.journal_number(&id), number);
    }
    renderer.color = true;
    assert!(record_clock(&renderer, &forwarded)
        .contains("\x1b[1;38;5;215m5:\x1b[0m\x1b[1;4;38;5;215m1\x1b[0m"));
}

#[test]
fn hundred_journal_clocks_wrap_whole_cells_without_losing_counters_or_highlighting() {
    let journals: Vec<_> = (1..=100)
        .map(|index| {
            let mut journal = fact(index, 100, &[], json!({})).journal;
            journal.stage.as_mut().unwrap().key = format!("stage_{index:03}");
            journal
        })
        .collect();
    let mut renderer = Renderer::new(&ViewArgs::default(), false, false, journals.iter());
    let mut record = fact(57, u64::MAX, &[], json!({}));
    let expected: Vec<_> = (1..=100)
        .map(|index| {
            let counter = if index == 57 || index % 11 == 0 {
                u64::MAX
            } else {
                index * 1000
            };
            if let RunRecordData::Chain(row) = &mut record.record {
                row.envelope
                    .provenance
                    .journal
                    .vector_clock
                    .clocks
                    .insert(coordinate(index), counter);
            }
            format!("{index}:{counter}")
        })
        .collect();
    for width in [40, 60, 90] {
        renderer.width = width;
        renderer.color = false;
        let plain = record_clock(&renderer, &record);
        assert!(plain.starts_with('⟨') && plain.ends_with('⟩'));
        assert!(
            plain.lines().all(|line| line.chars().count() <= width),
            "{plain}"
        );
        assert!(plain.lines().skip(1).all(|line| line.starts_with("  ")));
        let cells: Vec<_> = plain
            .lines()
            .flat_map(|line| {
                line.trim()
                    .trim_start_matches('⟨')
                    .trim_end_matches('⟩')
                    .split(',')
                    .filter(|cell| !cell.is_empty())
            })
            .collect();
        assert_eq!(
            cells, expected,
            "never split or omit a journal:counter cell"
        );
        renderer.color = true;
        let colored = record_clock(&renderer, &record);
        assert_eq!(colored.matches("\x1b[1;4;38;5;").count(), 1);
        assert!(colored.contains(&format!(
            "\x1b[1;38;5;215m57:\x1b[0m\x1b[1;4;38;5;215m{}\x1b[0m",
            u64::MAX
        )));
        let mut parts = colored.split("\x1b[");
        let mut stripped = parts.next().unwrap().to_owned();
        for part in parts {
            stripped.push_str(part.split_once('m').unwrap().1);
        }
        assert_eq!(stripped, plain, "ANSI colors must not change line wrapping");
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
        row.envelope.provenance.event.event_type = row
            .payload
            .framework_event_type(&row.envelope.provenance.event.flow_context.stage_name)
            .unwrap()
            .into();
        row.envelope.provenance.event.payload_schema_version =
            row.payload.framework_schema_version().unwrap();
    }
    record
}

#[test]
fn all_six_effect_occurrences_retain_their_names_and_evidence_in_human_and_jsonl() {
    use obzenflow_core::event::payloads::effect_payload::{
        EffectCursor, EffectDescriptor, EffectObservation, EffectOutcomePayload, EffectPortReturn,
        EffectRecord, EffectUnresolvedReason, ExternalCompletion, RetryDisposition,
    };
    let cases = [
        (
            EffectObservation::ExecutionRejected {},
            "effect.execution_rejected",
            false,
        ),
        (
            EffectObservation::returned_failure(),
            "effect.execution_failed",
            false,
        ),
        (
            EffectObservation::OutcomePreparationFailed {},
            "effect.outcome_preparation_failed",
            false,
        ),
        (
            EffectObservation::ExecutionUnresolved {
                reason: EffectUnresolvedReason::MissingTransactionalSettlement {
                    port_return: EffectPortReturn::Succeeded,
                },
                external_completion: ExternalCompletion::Unknown,
            },
            "effect.execution_unresolved",
            false,
        ),
        (
            EffectObservation::returned_success(),
            "effect.execution_succeeded",
            true,
        ),
        (
            EffectObservation::ValueCaptured {},
            "effect.value_captured",
            true,
        ),
    ];
    for (observation, name, success) in cases {
        let captured = matches!(observation, EffectObservation::ValueCaptured {});
        let body = EffectRecord {
            observation: observation.clone(),
            cursor: EffectCursor::new("flow", "classify", 1, 0),
            descriptor_hash: "fixture".into(),
            descriptor: EffectDescriptor::new(
                if captured {
                    "obzenflow.capture"
                } else {
                    "sensor.calibrate"
                },
                "calibrate",
                1,
                "1",
                "input",
            ),
            outcome: if success {
                EffectOutcomePayload::Succeeded { output: json!(7) }
            } else {
                EffectOutcomePayload::Failed {
                    error_type: "arbitrary_dependency_code".into(),
                    error_message: "diagnostic only".into(),
                    retry: RetryDisposition::NotRetryable,
                    cause: None,
                    detail: None,
                }
            },
            origin: None,
        };
        body.validate().unwrap();
        let record = execution(100, &[], ExecutionPayload::EffectRecord(body));
        for jsonl in [false, true] {
            let view = ViewArgs {
                jsonl,
                ..ViewArgs::default()
            };
            let mut renderer = Renderer::new(&view, false, false, [&record.journal].into_iter());
            let mut output = Vec::new();
            renderer.record(&mut output, record.clone()).unwrap();
            let output = String::from_utf8(output).unwrap();
            assert!(output.contains(name), "{output}");
            if jsonl {
                let row: RunRecord = serde_json::from_str(output.trim()).unwrap();
                let RunRecordData::Chain(row) = row.record else {
                    panic!("chain row")
                };
                let ChainPayload::Execution(ExecutionPayload::EffectRecord(body)) = &row.payload
                else {
                    panic!("effect row")
                };
                assert_eq!(body.observation, observation);
            } else {
                assert!(output.contains("payload schema version: 1"));
                assert!(output.contains("\"observation\""), "{output}");
            }
        }
    }
}

#[test]
fn primary_execution_failures_remain_visible_in_default_human_and_jsonl_views() {
    use obzenflow_core::event::{
        SinkOperationFailed, SinkOperationPhase, StageFatalCode, StageFatalReason,
        StageFatalRecorded, StageFatalSeverity,
    };
    let records = [
        execution(
            100,
            &[],
            ExecutionPayload::StageFatalRecorded(StageFatalRecorded {
                severity: StageFatalSeverity::Primary,
                stage_id: id(2).parse().unwrap(),
                stage_key: "classify".into(),
                causal_event_id: None,
                input_position: None,
                primary_cause_event_id: None,
                code: StageFatalCode::Journal,
                reason: StageFatalReason::JournalFailure,
                detail: "journal append failed".into(),
                departure: None,
            }),
        ),
        execution(
            101,
            &[],
            ExecutionPayload::SinkOperationFailed(SinkOperationFailed {
                stage_id: id(2).parse().unwrap(),
                stage_key: "classify".into(),
                logical_destination: "console".into(),
                causal_event_id: None,
                input_position: None,
                failed_delivery_event_id: None,
                operation_subject_event_id: None,
                phase: SinkOperationPhase::Flush,
                kind: obzenflow_core::event::status::processing_status::ErrorKind::Remote,
                destination_error_code: None,
                detail: "stream flush failed".into(),
            }),
        ),
    ];
    for jsonl in [false, true] {
        let view = ViewArgs {
            jsonl,
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
        let output = String::from_utf8(output).unwrap();
        assert!(
            output.contains("obzenflow.stage_fatal_recorded"),
            "{output}"
        );
        assert!(
            output.contains("obzenflow.sink_operation_failed"),
            "{output}"
        );
        if jsonl {
            let rows: Vec<RunRecord> = output
                .lines()
                .map(|row| serde_json::from_str(row).unwrap())
                .collect();
            assert_eq!(rows.len(), 2);
            for (row, version) in rows.iter().zip([1, 2]) {
                let RunRecordData::Chain(record) = &row.record else {
                    panic!("chain row")
                };
                assert_eq!(
                    record
                        .envelope
                        .provenance
                        .event
                        .payload_schema_version
                        .get(),
                    version
                );
            }
        }
    }
}

#[test]
fn a_departure_reaches_jsonl_and_the_human_view_keeps_its_detail() {
    use obzenflow_core::event::payloads::effect_payload::{
        EffectInputPosition, EffectOrdinal, EffectType,
    };
    use obzenflow_core::event::{
        Departure, DepartureAction, StageFatalCode, StageFatalReason, StageFatalRecorded,
        StageFatalSeverity,
    };
    let detail = "stage 'score' would perform Score at input 3, but its recording settled \
                  that input without an action";
    let departure = Departure {
        offered: Some(DepartureAction::Effect {
            effect_type: EffectType::from("app_identity.score"),
            input_position: EffectInputPosition::new(3),
            effect_ordinal: EffectOrdinal::new(0),
        }),
        expected: DepartureAction::Settlement {
            input_position: EffectInputPosition::new(3),
        },
    };
    let record = execution(
        100,
        &[],
        ExecutionPayload::StageFatalRecorded(StageFatalRecorded {
            severity: StageFatalSeverity::Primary,
            stage_id: id(2).parse().unwrap(),
            stage_key: "score".into(),
            causal_event_id: None,
            input_position: Some(3),
            primary_cause_event_id: None,
            code: StageFatalCode::Replay,
            reason: StageFatalReason::MissingRecordedEffect,
            detail: detail.into(),
            departure: Some(departure.clone()),
        }),
    );
    for jsonl in [false, true] {
        let view = ViewArgs {
            jsonl,
            ..ViewArgs::default()
        };
        let mut renderer = Renderer::new(&view, false, false, std::iter::once(&record.journal));
        let mut output = Vec::new();
        renderer.record(&mut output, record.clone()).unwrap();
        let output = String::from_utf8(output).unwrap();
        if jsonl {
            let row: RunRecord = serde_json::from_str(output.trim_end()).unwrap();
            let RunRecordData::Chain(row) = &row.record else {
                panic!("chain row")
            };
            let ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)) = &row.payload
            else {
                panic!("stage fatal row")
            };
            assert_eq!(fatal.departure.as_ref(), Some(&departure));
            assert_eq!(fatal.detail, detail);
        } else {
            assert!(output.contains(detail), "{output}");
        }
    }
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
                assert!(output.contains("sensor.reading ←"));
                assert!(output.contains("sensor.classified ←"));
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
        with_inputs(
            fact(2, 104, &[102], json!({"n":3})),
            &[(102, &progress_type)],
        ),
    ] {
        renderer.record(&mut output, record).unwrap();
    }
    let text = String::from_utf8(output).unwrap();
    assert!(!text.contains("RUNTIME") && !text.contains("inputs_since_last_report"));
    assert_eq!(
        text.matches("TRANSFORM (stage: classify, journal: 2)\nsensor.classified ← ")
            .count(),
        3,
        "a hidden journal boundary must still separate facts: {text}"
    );
    assert!(!text.contains(&format!("\n{progress_type} ← ")));
    assert!(text.contains(&format!("sensor.classified ← classify({progress_type})")));
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
            "observation":{"observation":"execution_failed","authority":"port_return","external_completion":"unknown"},
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
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("\x1b[38;5;217mEFFECT (stage: \x1b[0m\x1b[1;38;5;231mclassify\x1b[0m\x1b[38;5;217m, journal: 2)\x1b[0m\n\x1b[1;38;5;224meffect.execution_failed\x1b[0m\x1b[38;5;217m ← classify(sensor.reading)\x1b[0m"));
        assert!(
            text.contains("\"outcome\": \"failed\"")
                && text.contains("\"effect_type\": \"sensor.calibrate\"")
        );
        assert!(text.contains("\"error_message\": \"calibration unavailable\""));
        assert!(text.contains("\x1b[1;38;5;208mEFFECTFUL TRANSFORM (stage: \x1b[0m\x1b[1;38;5;223mclassify\x1b[0m\x1b[1;38;5;208m, journal: 2)\x1b[0m\n\x1b[1;38;5;215msensor.classified\x1b[0m\x1b[1;38;5;208m ← classify(sensor.reading)\x1b[0m"));
        assert_eq!(text.contains("RUNTIME"), verbose);
        if verbose {
            let runtime = text
                .split("\x1b[38;5;245mRUNTIME")
                .nth(1)
                .unwrap()
                .split("\n\n")
                .next()
                .unwrap();
            assert!(runtime.contains("\x1b[38;5;245m  \"inputs_since_last_report\": 1"));
            assert!(!runtime.contains("execution_type"));
            assert!(runtime.contains(
                "(stage: \x1b[0m\x1b[1;38;5;255mclassify\x1b[0m\x1b[38;5;245m, journal: 2)"
            ));
            assert!(runtime.contains(&format!("\x1b[1;38;5;250m{}\x1b[0m", event_type(&progress))));
            assert!(runtime.contains("\x1b[1;38;5;250m2:\x1b[0m\x1b[1;4;38;5;250m102\x1b[0m"));
            assert!(!runtime.contains("38;5;208m") && !runtime.contains("38;5;217m"));
        }
    }
}

#[test]
fn declared_effectful_stage_kinds_apply_before_any_effect_has_run() {
    for (stage_type, heading, color, output_color, reporter_color) in [
        (StageType::Transform, "EFFECTFUL TRANSFORM", 208, 215, 223),
        (StageType::Stateful, "EFFECTFUL STATEFUL", 114, 157, 194),
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
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains(&format!(
            "\x1b[1;38;5;{color}m{heading} (stage: \x1b[0m\x1b[1;38;5;{reporter_color}mclassify\x1b[0m\x1b[1;38;5;{color}m, journal: 2)\x1b[0m\n\x1b[1;38;5;{output_color}msensor.classified\x1b[0m\x1b[1;38;5;{color}m ← classify(sensor.reading)\x1b[0m"
        )), "{text}");
        assert!(
            !text.contains("mEFFECT (stage:"),
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
        let text = String::from_utf8(output).unwrap();
        let headings: Vec<_> = text
            .split("\n\n")
            .filter_map(|block| block.lines().next())
            .collect();
        assert_eq!(
            headings,
            [
                "SOURCE (stage: thermometer, journal: 1)".to_owned(),
                format!("{expected} (stage: classify, journal: 2)"),
                format!("{expected} (stage: classify, journal: 2)"),
            ]
        );
    }
}

#[test]
fn ninety_columns_keeps_long_output_expressions_together_and_narrow_views_still_wrap() {
    let expression =
        "payment.authorization_unavailable ← authorize_payment(payment.order_validated)";
    for width in [60, 90] {
        let mut renderer = renderer();
        renderer.width = width;
        let mut input = fact(1, 100, &[], json!({}));
        if let RunRecordData::Chain(row) = &mut input.record {
            row.envelope.provenance.event.event_type = "payment.order_validated".into();
        }
        let mut result = with_inputs(
            fact(2, 101, &[100], json!({"reason":"unavailable"})),
            &[(100, "payment.order_validated")],
        );
        let stage = result.journal.stage.as_mut().unwrap();
        stage.key = "authorize_payment".into();
        stage.is_effectful = true;
        if let RunRecordData::Chain(row) = &mut result.record {
            row.envelope.provenance.event.event_type = "payment.authorization_unavailable".into();
        }
        let mut output = Vec::new();
        renderer.record(&mut output, input).unwrap();
        renderer.record(&mut output, result).unwrap();
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
fn emphasis_preserves_wrapped_unicode_stage_and_event_names() {
    let stage_name = "étape_paiement_".repeat(5);
    let output_type = format!("{}v1", "paiement.autorisé.".repeat(5));
    let mut record = fact(2, 101, &[], json!({"amount": 42}));
    record.journal.stage.as_mut().unwrap().key = stage_name.clone();
    if let RunRecordData::Chain(row) = &mut record.record {
        row.envelope.provenance.event.event_type = output_type.clone();
    }
    for width in [40, 90] {
        let mut renderer = Renderer::new(
            &ViewArgs::default(),
            false,
            false,
            [&record.journal].into_iter(),
        );
        renderer.width = width;
        let mut plain = Vec::new();
        renderer
            .terminal()
            .render(
                &mut plain,
                &EventView::from_record(&record, &renderer.context),
            )
            .unwrap();
        renderer.color = true;
        let mut colored = Vec::new();
        renderer
            .terminal()
            .render(
                &mut colored,
                &EventView::from_record(&record, &renderer.context),
            )
            .unwrap();
        let colored = String::from_utf8(colored).unwrap();
        let mut pieces = colored.split("\x1b[");
        let mut stripped = pieces.next().unwrap().to_owned();
        for piece in pieces {
            stripped.push_str(piece.split_once('m').unwrap().1);
        }
        assert_eq!(stripped, String::from_utf8(plain).unwrap());
        assert!(stripped
            .lines()
            .filter(|line| !line.starts_with('⟨'))
            .all(|line| line.chars().count() <= width));
        let (heading, _) = colored.split_once("\x1b[1;38;5;215m").unwrap();
        let highlighted_stage = heading
            .split("\x1b[1;38;5;223m")
            .skip(1)
            .map(|piece| piece.split_once("\x1b[0m").unwrap().0)
            .collect::<String>();
        assert_eq!(highlighted_stage, stage_name);
        let highlighted_event = colored
            .split_once('⟨')
            .unwrap()
            .0
            .split("\x1b[1;38;5;215m")
            .skip(1)
            .map(|piece| piece.split_once("\x1b[0m").unwrap().0)
            .collect::<String>();
        assert_eq!(highlighted_event, output_type);
    }
}

#[test]
fn multiple_outputs_each_lead_with_their_own_type_clock_and_payload() {
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
        row.envelope.provenance.event.event_type = "sensor.cooling_requested".into();
    }
    renderer
        .record(&mut output, fact(1, 100, &[], json!({"celsius":38})))
        .unwrap();
    renderer.record(&mut output, first).unwrap();
    renderer.record(&mut output, second).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.contains(&format!(
            "SOURCE (stage: thermometer, journal: 1)\nsensor.reading ← thermometer()\n{}",
            displayed_clock(1, 100)
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "TRANSFORM (stage: classify, journal: 2)\nsensor.classified ← classify(sensor.reading)\n{}",
            displayed_clock(2, 101)
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "TRANSFORM (stage: classify, journal: 2)\nsensor.cooling_requested ← classify(sensor.reading)\n{}",
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
        .find("sensor.cooling_requested ← classify(sensor.reading)")
        .unwrap();
    assert!(
        last_fact < last_clock,
        "each fact retains its own clock below its typed equation"
    );
    assert_eq!(
        text.matches("\"sensor\": \"A\"").count(),
        2,
        "each fact keeps its own payload, even when fields match: {text}"
    );
    for (event_type, reason) in [
        ("sensor.classified", json!("too_hot")),
        ("sensor.cooling_requested", json!({"cooling":"requested"})),
    ] {
        let block = text
            .split("\n\n")
            .find(|block| {
                block.starts_with(&format!(
                    "TRANSFORM (stage: classify, journal: 2)\n{event_type} ← "
                ))
            })
            .unwrap();
        let payload_start = block.find("\n{\n").unwrap() + 1;
        let payload: Value = serde_json::from_str(&block[payload_start..]).unwrap();
        assert_eq!(payload, json!({"sensor":"A", "reason":reason}));
    }
    assert_eq!(renderer.records, 3, "every record is counted");
}

#[test]
fn missing_parents_flush_naming_only_recorded_input_types_without_grouping_unrelated_facts() {
    let mut renderer = renderer();
    let mut output = Vec::new();
    renderer
        .record(&mut output, fact(1, 100, &[], json!({"n":1})))
        .unwrap();
    renderer
        .record(&mut output, fact(2, 101, &[98], json!({"n":1})))
        .unwrap();
    renderer
        .record(
            &mut output,
            with_inputs(fact(2, 102, &[99], json!({"n":1})), &[]),
        )
        .unwrap();
    let text = String::from_utf8(output).unwrap();
    assert_eq!(
        text.matches("TRANSFORM (stage: classify, journal: 2)\nsensor.classified ← ")
            .count(),
        2,
        "{text}"
    );
    assert_eq!(
        text.lines()
            .filter(|line| matches!(
                *line,
                "SOURCE (stage: thermometer, journal: 1)"
                    | "TRANSFORM (stage: classify, journal: 2)"
            ))
            .count(),
        3,
        "matching values do not establish shared parents"
    );
    assert_eq!(
        text.matches("sensor.classified ← classify(sensor.reading)")
            .count(),
        1,
        "a missing parent's type comes from the record that names it: {text}"
    );
    assert_eq!(
        text.matches("sensor.classified ← classify(input not recorded)")
            .count(),
        1,
        "no type is taken from anywhere but the record: {text}"
    );
}

#[tokio::test]
async fn explanation_shows_inherited_clock_without_waiting_for_other_journals() {
    use obzenflow_core::journal::{AppendOptions, Journal};
    use obzenflow_infra::journal::MemoryJournal;

    let mut source = fact(1, 100, &[], json!({}));
    let source_journal = MemoryJournal::with_owner_in_run(
        obzenflow_core::JournalOwner::stage(source.journal.stage.as_ref().unwrap().id),
        source.run.flow_id,
    );
    let RunRecordData::Chain(row) = &source.record else {
        unreachable!()
    };
    let source_receipt = source_journal
        .append(row.authored(), Default::default())
        .await
        .unwrap();
    let input = AppendOptions::from_record(Some(&source_receipt)).unwrap();
    source.journal.id = *source_journal.id();
    source.record = RunRecordData::Chain(Box::new(source_receipt));

    let mut child = fact(2, 101, &[], json!({}));
    let child_journal = MemoryJournal::with_owner_in_run(
        obzenflow_core::JournalOwner::stage(child.journal.stage.as_ref().unwrap().id),
        child.run.flow_id,
    );
    let RunRecordData::Chain(row) = &child.record else {
        unreachable!()
    };
    let child_receipt = child_journal.append(row.authored(), input).await.unwrap();
    child.journal.id = *child_journal.id();
    child.record = RunRecordData::Chain(Box::new(child_receipt));

    let mut renderer = Renderer::new(
        &ViewArgs::default(),
        false,
        false,
        [&source.journal, &child.journal].into_iter(),
    );
    renderer.begin(&mut Vec::new(), &source.run, false).unwrap();
    renderer.explain = true;
    renderer.full = true;
    let mut output = Vec::new();
    renderer.record(&mut output, child).unwrap();
    let first = std::str::from_utf8(&output).unwrap();
    assert!(first.contains("⟨1:1,2:1⟩"), "{first}");
    renderer.record(&mut output, source).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert_eq!(renderer.records, 2);
    assert!(text.contains("⟨1:1⟩"), "{text}");
    assert!(!text.contains("causal proof"), "{text}");
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
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("⟨1:100,3:12⟩"), "{text}");
    assert!(text.contains(&format!("3 journal_{}", id(3))), "{text}");
    assert!(text.contains("input not recorded"), "{text}");
}

#[test]
fn clocks_name_journals_from_earlier_runs_in_the_lineage() {
    let mut renderer = renderer();
    let mut ancestor = fact(1, 100, &[], json!({})).journal;
    ancestor.id = *coordinate(3).journal_writer_id.as_journal_id();
    renderer.name_ancestor_journals([(ancestor, "gen 0".to_string())]);
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
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("⟨1:100,3:12⟩"), "{text}");
    assert!(text.contains("3 thermometer (gen 0)"), "{text}");
    assert!(!text.contains(&format!("journal_{}", id(3))), "{text}");
    let matrix = matrix_text(&renderer, &RunReadProgress::default());
    assert!(
        matrix
            .lines()
            .any(|line| line.split_whitespace().collect::<Vec<_>>()
                == ["3", "thermometer", "(gen", "0)", "—", "—", "—"]),
        "{matrix}"
    );
}

#[test]
fn replayed_facts_name_their_original_author_by_the_stage_that_owns_the_journal() {
    let mut renderer = renderer();
    let mut replayed = fact(1, 100, &[], json!({}));
    let mut unrelated = fact(1, 101, &[], json!({}));
    for (record, writer) in [(&mut replayed, 91), (&mut unrelated, 92)] {
        let RunRecordData::Chain(row) = &mut record.record else {
            unreachable!()
        };
        row.envelope.provenance.event.writer_id =
            serde_json::from_value(json!({"type": "Stage", "id": id(writer)})).unwrap();
    }
    let RunRecordData::Chain(row) = &mut replayed.record else {
        unreachable!()
    };
    row.envelope.provenance.event.replay_context = Some(
        serde_json::from_value(json!({
            "original_event_id": id(100),
            "original_flow_id": format!("flow_{}", id(90)),
            "original_stage_id": id(91),
        }))
        .unwrap(),
    );
    let replayed_writer = writer_id(&replayed);
    let unrelated_writer = writer_id(&unrelated);
    let mut output = Vec::new();
    renderer.record(&mut output, replayed).unwrap();
    renderer.record(&mut output, unrelated).unwrap();
    assert_eq!(
        renderer.context.writer_name(&replayed_writer),
        "thermometer [replay]"
    );
    assert_eq!(
        renderer.context.writer_name(&unrelated_writer),
        unrelated_writer
    );
    let live = fact(2, 102, &[100], json!({}));
    assert_eq!(renderer.context.writer_name(&writer_id(&live)), "classify");
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
fn reporting_journal_highlight_uses_identity_and_row_color_instead_of_counter_size() {
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
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.contains("\x1b[1;4;38;5;215m9\x1b[0m"),
        "source counter matches the emphasized event color"
    );
    assert!(
        text.contains("\x1b[1;4;38;5;215m10\x1b[0m"),
        "transform counter matches the emphasized event color"
    );
    assert!(
        text.contains("\x1b[38;5;245m2:999\x1b[0m"),
        "another journal's larger history stays gray"
    );
    assert!(
        text.contains("\x1b[38;5;245m1:100\x1b[0m"),
        "merged history stays gray"
    );
    for (stage, number, counter) in [("thermometer", 1, 9), ("classify", 2, 10)] {
        assert_eq!(
            text.matches(&format!("\x1b[1;38;5;223m{stage}\x1b[0m"))
                .count(),
            1,
            "only the heading's stage name uses the brightest shade"
        );
        assert!(text.contains(&format!(
            "\x1b[1;38;5;215m{number}:\x1b[0m\x1b[1;4;38;5;215m{counter}\x1b[0m"
        )));
    }
    assert!(text.contains(
        "\x1b[1;38;5;208mSOURCE (stage: \x1b[0m\x1b[1;38;5;223mthermometer\x1b[0m\x1b[1;38;5;208m, journal: 1)\x1b[0m\n\x1b[1;38;5;215msensor.reading\x1b[0m\x1b[1;38;5;208m ← thermometer()\x1b[0m"
    ));
    assert!(
        text.contains("\x1b[1;38;5;208mTRANSFORM (stage: \x1b[0m\x1b[1;38;5;223mclassify\x1b[0m\x1b[1;38;5;208m, journal: 2)\x1b[0m\n\x1b[1;38;5;215msensor.classified\x1b[0m\x1b[1;38;5;208m ← classify(sensor.reading)\x1b[0m")
    );
    for line in text.lines().filter(|line| !line.contains('⟨')) {
        assert!(
            line.matches('\x1b').count()
                <= if line.contains("(stage:") {
                    6
                } else if line.contains('←') {
                    4
                } else {
                    2
                },
            "headings emphasize the stage and equations emphasize the event: {line}"
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
            row.envelope.provenance.event.event_type = "sensor.calibration".into();
        }
        let mut result = with_inputs(
            fact(2, 101, &[99, 100], json!({"celsius":40})),
            &[(99, "sensor.calibration"), (100, "sensor.reading")],
        );
        result.journal.stage.as_mut().unwrap().stage_type = stage_type;
        if let RunRecordData::Chain(row) = &mut result.record {
            row.envelope.provenance.event.flow_context.stage_type = stage_type;
        }
        let mut output = Vec::new();
        for record in [catalog, fact(1, 100, &[], json!({"celsius":38})), result] {
            renderer.record(&mut output, record).unwrap();
        }
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains(&format!(
            "\x1b[1;38;5;114m{heading} (stage: \x1b[0m\x1b[1;38;5;194mclassify\x1b[0m\x1b[1;38;5;114m, journal: 2)\x1b[0m\n\x1b[1;38;5;157msensor.classified\x1b[0m\x1b[1;38;5;114m ← classify(sensor.calibration, sensor.reading)\x1b[0m"
        )), "{text}");
        assert!(text.contains("\x1b[1;4;38;5;157m101\x1b[0m"));
        assert!(text.contains("  \"celsius\": 40"));
        assert!(!text.contains("state =") && !text.contains("state'"));
    }
}

#[test]
fn a_linked_stateful_output_names_its_input_and_its_previous_output() {
    let stateful = |event: u64, parents: &[u64]| {
        let mut record = fact(2, event, parents, json!({"celsius": event}));
        record.journal.stage.as_mut().unwrap().stage_type = StageType::Stateful;
        if let RunRecordData::Chain(row) = &mut record.record {
            row.envelope.provenance.event.flow_context.stage_type = StageType::Stateful;
        }
        record
    };
    let mut renderer = renderer();
    let mut output = Vec::new();
    for record in [
        fact(1, 100, &[], json!({"celsius":38})),
        stateful(101, &[100]),
        fact(1, 102, &[], json!({"celsius":40})),
        with_inputs(
            stateful(103, &[102, 101]),
            &[(102, "sensor.reading"), (101, "sensor.classified")],
        ),
    ] {
        renderer.record(&mut output, record).unwrap();
    }
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.contains(&format!(
            "STATEFUL (stage: classify, journal: 2)\nsensor.classified ← classify(sensor.reading)\n{}",
            displayed_clock(2, 101)
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "STATEFUL (stage: classify, journal: 2)\nsensor.classified ← classify(sensor.reading, sensor.classified)\n{}",
            displayed_clock(2, 103)
        )),
        "{text}"
    );
}

#[test]
fn arbitrary_payload_shapes_keep_types_units_and_escape_terminal_controls() {
    let value = json!({
        "amount_cents":1234, "nested":{"label":"a\u{1b}[2J\nb\u{202e}"},
        "items":[1,2,3,4,5], "numeric_string":"1234", "enabled":true
    });
    let text = pretty(&value).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), value);
    assert!(text.starts_with("{\n  \"amount_cents\": 1234,"));
    assert!(!text.contains('·') && !text.contains('\x1b') && !text.contains('\u{202e}'));
    assert!(text.lines().all(|line| line.chars().count() <= 80));
    assert_eq!(pretty(&json!(42)).unwrap(), "42");
    assert_eq!(pretty(&Value::Null).unwrap(), "null");
    assert_eq!(pretty(&json!({})).unwrap(), "{}");
    assert_eq!(pretty(&json!([])).unwrap(), "[]");
}

#[test]
fn long_json_strings_preserve_values_and_only_compact_output_abbreviates() {
    let value = json!({"reason":{"nested":["é\"\\\n\u{202e}".repeat(30)]}});
    let text = pretty(&value).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), value);
    assert!(text.lines().any(|line| line.chars().count() > 90));
    for width in [40, 60, 80, 90] {
        let preview = abbreviated(&compact(&value).unwrap(), width);
        assert!(preview.ends_with("… [--full]"));
        assert!(preview.chars().count() <= width);
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
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("[processing error]"));
    assert!(text.contains("processing error: calibration unavailable"));
    assert!(text.contains("\"sensor\": \"A\""));
}

fn join_row(event: u64, event_kind: &str, event_type: &str, payload: Value) -> RunRecord {
    let kind = if event_kind == "fact" {
        "stage_output"
    } else {
        "execution"
    };
    serde_json::from_value(json!({
        "version": 3,
        "run": {"flow_id": id(10), "pipeline_writer_id": {"type":"System", "id":id(11)}},
        "journal": {"id":id(5), "kind":"data", "stage":{"key":"posted", "id":id(5), "stage_type":"Join","is_effectful":false}},
        "position":event,
        "kind":kind,
        "record": {
            "envelope": {"provenance": {
                "event": {
                    "id":id(event), "writer_id":{"type":"Stage", "id":id(5)},
                    "event_kind":event_kind, "event_type":event_type, "payload_schema_version":1,
                    "causality":{"parent_ids":[]},
                    "flow_context":{"flow_name":"bank", "flow_id":id(10), "stage_name":"posted", "stage_id":id(5), "stage_type":"Join"},
                    "processing":{"event_time":0, "status":"Success"}, "composite_activations":[]
                },
                "journal": {
                    "journal_writer_id":id(5),
                    "run_id":id(10),
                    "previous":{"run_id":id(10),"journal_writer_id":id(5),"sequence":event-1,"event_id":id(event-1)},
                    "vector_clock":{"entries":[{"journal_writer_id":id(5),"sequence":event}]},
                    "timestamp":"2026-10-14T00:00:00Z", "journal_group_id":null, "journal_group_member":null
                }
            }},
            "payload":payload
        }
    })).unwrap()
}

fn phase_row(event: u64, phase: &str, generation: u64) -> RunRecord {
    join_row(
        event,
        "execution",
        &format!("runtime.join.{phase}"),
        json!({"execution_type":"join_phase", "phase":phase, "generation":generation, "ended_by":["batch_tx"]}),
    )
}

fn posting(event: u64) -> RunRecord {
    join_row(event, "fact", "bank.posted_entry", json!({"n": event}))
}

#[test]
fn join_facts_are_headed_with_the_phase_their_join_had_journalled() {
    let first = posting(201);
    let mut renderer = Renderer::new(
        &ViewArgs::default(),
        false,
        false,
        [&first.journal].into_iter(),
    );
    renderer.width = 160;
    let mut output = Vec::new();
    for record in [
        first,
        phase_row(202, "enriching_entered", 0),
        posting(203),
        phase_row(204, "live_entered", 1),
        posting(205),
    ] {
        renderer.record(&mut output, record).unwrap();
    }
    let headings: Vec<String> = String::from_utf8(output)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("JOIN (stage: posted"))
        .map(str::to_owned)
        .collect();
    assert_eq!(headings.len(), 3, "{headings:?}");
    assert!(!headings[0].contains("phase:"), "{headings:?}");
    assert!(
        headings[1].ends_with(", phase: enriching since generation 0)"),
        "{headings:?}"
    );
    assert!(
        headings[2].ends_with(", phase: live since generation 1)"),
        "{headings:?}"
    );
}

fn on_stage(mut record: RunRecord, stage: u64, key: &str, stage_type: StageType) -> RunRecord {
    let stage_id = serde_json::from_value(json!(id(stage))).unwrap();
    let owner = record.journal.stage.as_mut().unwrap();
    owner.key = key.into();
    owner.id = stage_id;
    owner.stage_type = stage_type;
    record.journal.id = serde_json::from_value(json!(id(stage))).unwrap();
    if let RunRecordData::Chain(row) = &mut record.record {
        row.envelope.provenance.event.writer_id = obzenflow_core::WriterId::from(stage_id);
        row.envelope.provenance.event.flow_context.stage_name = key.into();
        row.envelope.provenance.event.flow_context.stage_id = stage_id;
    }
    record
}

fn in_error_journal(mut record: RunRecord, payload: ExecutionPayload) -> RunRecord {
    use obzenflow_core::event::status::processing_status::ErrorKind;
    let owner = record.journal.stage.as_ref().unwrap().id;
    let (message, kind): (String, ErrorKind) = match &payload {
        ExecutionPayload::StageProcessingFailed(failure) => {
            (failure.detail.clone(), failure.kind.clone())
        }
        ExecutionPayload::SourceRecordRejected(rejected) => {
            (rejected.detail.clone(), rejected.kind.clone())
        }
        ExecutionPayload::SinkOperationFailed(failure) => {
            (failure.detail.clone(), failure.kind.clone())
        }
        _ => unreachable!("error records only"),
    };
    record.kind = RunRecordKind::Execution;
    record.journal.kind = RunJournalKind::Error;
    record.journal.id =
        serde_json::from_value(json!(format!("7{}", &owner.as_ulid().to_string()[1..]))).unwrap();
    if let RunRecordData::Chain(row) = &mut record.record {
        row.payload = ChainPayload::Execution(payload);
        row.envelope.provenance.event.event_kind = row.payload.kind();
        row.envelope.provenance.event.event_type = row
            .payload
            .framework_event_type(&row.envelope.provenance.event.flow_context.stage_name)
            .unwrap()
            .into();
        row.envelope.provenance.event.payload_schema_version =
            row.payload.framework_schema_version().unwrap();
        row.envelope.provenance.event.processing.status = ProcessingStatus::Error {
            message,
            kind: Some(kind),
        };
    }
    record
}

fn processing_failed(stage: u64, event: u64, input: u64, detail: &str) -> RunRecord {
    use obzenflow_core::event::payloads::stage_processing_payload::{
        FailedInput, StageProcessingFailed,
    };
    use obzenflow_core::event::status::processing_status::ErrorKind;
    in_error_journal(
        fact(stage, event, &[input], json!({})),
        ExecutionPayload::StageProcessingFailed(StageProcessingFailed {
            kind: ErrorKind::Validation,
            detail: detail.into(),
            input: FailedInput {
                event_type: obzenflow_core::EventType("sensor.reading".into()),
                payload_schema_version: std::num::NonZeroU32::MIN,
                payload: json!({"celsius": null, "id": 2}),
            },
        }),
    )
}

fn rejected_row(event: u64, index: u64) -> RunRecord {
    use obzenflow_core::event::payloads::execution_payload::SourceRecordRejectedFact;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason};
    let diagnostic = SourceDiagnostic::new(SourceDiagnosticReason::InvalidRecord)
        .record(index)
        .position(std::num::NonZeroU32::new(index as u32 + 2).unwrap(), None);
    in_error_journal(
        fact(1, event, &[], json!({})),
        ExecutionPayload::SourceRecordRejected(SourceRecordRejectedFact {
            kind: ErrorKind::Validation,
            detail: format!("record {index}: {diagnostic}"),
            diagnostic,
        }),
    )
}

fn receipt(stage: u64, event: u64, input: &RunRecord, result: Value) -> RunRecord {
    let mut record = on_stage(
        fact(stage, event, &[], json!({})),
        stage,
        "written",
        StageType::Sink,
    );
    record.kind = RunRecordKind::Delivery;
    let input_id = event_id(input);
    if let RunRecordData::Chain(row) = &mut record.record {
        let payload: obzenflow_core::event::payloads::delivery_payload::DeliveryPayload =
            serde_json::from_value(json!({
                "subject": {
                    "input": {
                        "run_id": id(10),
                        "journal_writer_id": id(2),
                        "sequence": 1,
                        "event_id": input_id
                    },
                    "event_kind": "fact",
                    "event_type": "sensor.classified",
                    "payload_schema_version": 1
                },
                "result": result,
                "destination": "written",
                "delivery_method": "noop",
                "bytes_processed": null,
                "processed_at": "2026-09-23T00:00:00Z"
            }))
            .unwrap();
        row.envelope.provenance.event.event_type = payload.event_type().into();
        row.payload = ChainPayload::Delivery(payload);
        row.envelope.provenance.event.event_kind = row.payload.kind();
        row.envelope.provenance.event.causality.parent_ids =
            vec![serde_json::from_value(json!(input_id)).unwrap()];
    }
    record
}

fn manifest_for(records: &[&RunRecord]) -> RunManifest {
    let stages: serde_json::Map<String, Value> = records
        .iter()
        .filter_map(|record| record.journal.stage.as_ref())
        .map(|stage| {
            (
                stage.key.clone(),
                json!({
                    "dsl_var": stage.key,
                    "stage_type": stage.stage_type,
                    "is_effectful": false,
                    "stage_id": stage.id.to_string(),
                    "stage_logic_version": "1",
                    "data_journal_file": format!("{}.log", stage.key),
                    "error_journal_file": format!("{}.errors.log", stage.key),
                    "inbound": [],
                    "delivery_order": "single_input"
                }),
            )
        })
        .collect();
    serde_json::from_value(json!({
        "journal_schema_version": obzenflow_core::journal::JOURNAL_SCHEMA_VERSION,
        "obzenflow_version": "test",
        "flow_id": id(10),
        "pipeline_writer_id": records[0].run.pipeline_writer_id,
        "flow_name": "sensors",
        "created_at": "2026-09-23T00:00:00Z",
        "stages": stages,
        "system_journal_file": "system.log",
        "metrics_journals": null
    }))
    .unwrap()
}

fn rendered_with_summary(renderer: &mut Renderer, records: &[RunRecord]) -> String {
    let mut output = Vec::new();
    for record in records {
        renderer.record(&mut output, record.clone()).unwrap();
    }
    String::from_utf8(output).unwrap() + &summary_text(renderer, records)
}

fn summary_text(renderer: &mut Renderer, records: &[RunRecord]) -> String {
    let manifest = manifest_for(&records.iter().collect::<Vec<_>>());
    let mut output = Vec::new();
    renderer
        .finish(
            &mut output,
            &mut Vec::new(),
            Observed {
                run: &records[0].run,
                manifest: &manifest,
                progress: &RunReadProgress::default(),
                lineage: Ok(RunLineage::default()),
                config_changes: Ok(Vec::new()),
            },
            ObservationEnd::Snapshot,
        )
        .unwrap();
    String::from_utf8(output).unwrap()
}

fn renderer_over(records: &[RunRecord]) -> Renderer {
    let mut renderer = Renderer::new(
        &ViewArgs::default(),
        false,
        false,
        records.iter().map(|record| &record.journal),
    );
    renderer.width = 90;
    renderer
}

#[test]
fn a_failed_input_displays_whole_under_its_stages_error_heading() {
    let input = fact(1, 100, &[], json!({"celsius": null, "id": 2}));
    let failed = processing_failed(2, 102, 100, "reading 2 has no temperature");
    let records = [input, failed];
    let mut renderer = renderer_over(&records);
    let text = rendered_with_summary(&mut renderer, &records);
    let block = text
        .split_once("TRANSFORM ERROR (stage: classify, error journal: ")
        .expect("error heading")
        .1;
    let lines: Vec<_> = block.lines().collect();
    assert_eq!(
        lines[1],
        "runtime.stage.processing_failed ← classify(sensor.reading)"
    );
    assert_eq!(lines[3], "payload schema version: 1");
    assert_eq!(
        lines[4..16].join("\n"),
        r#"{
  "kind": "Validation",
  "detail": "reading 2 has no temperature",
  "input": {
    "event_type": "sensor.reading",
    "payload_schema_version": 1,
    "payload": {
      "celsius": null,
      "id": 2
    }
  }
}"#
    );
    assert!(!text.contains("processing error:"));
    assert!(!text.contains("[processing error]"));

    let mut jsonl = Renderer::new(
        &ViewArgs {
            jsonl: true,
            ..ViewArgs::default()
        },
        false,
        false,
        records.iter().map(|record| &record.journal),
    );
    let mut output = Vec::new();
    jsonl.record(&mut output, records[1].clone()).unwrap();
    let line: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(
        line["record"]["payload"]["execution_type"], "stage_processing_failed",
        "JSONL keeps the tag the payload block leaves out"
    );
    assert!(line.get("repeat_of").is_none());
}

#[test]
fn a_refused_catalog_row_displays_as_its_joins_error() {
    use obzenflow_core::event::payloads::stage_processing_payload::{
        FailedInput, StageProcessingFailed,
    };
    use obzenflow_core::event::status::processing_status::ErrorKind;

    let row = fact(1, 100, &[], json!({"celsius": null, "id": 2}));
    let refused = in_error_journal(
        on_stage(
            fact(3, 103, &[100], json!({})),
            3,
            "calibrate",
            StageType::Join,
        ),
        ExecutionPayload::StageProcessingFailed(StageProcessingFailed {
            kind: ErrorKind::Validation,
            detail: "reading 2 has no temperature".into(),
            input: FailedInput {
                event_type: obzenflow_core::EventType("sensor.reading".into()),
                payload_schema_version: std::num::NonZeroU32::MIN,
                payload: json!({"celsius": null, "id": 2}),
            },
        }),
    );
    let records = [row, refused];
    let mut renderer = renderer_over(&records);
    let text = rendered_with_summary(&mut renderer, &records);
    let block = text
        .split_once("JOIN ERROR (stage: calibrate, error journal: ")
        .expect("join error heading")
        .1;
    assert_eq!(
        block.lines().nth(1),
        Some("runtime.stage.processing_failed ← calibrate(sensor.reading)")
    );
    let table = text.split_once("\nJOURNALS\n").unwrap().1;
    assert!(
        table
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .any(|line| line == "calibrate 0 1 Validation"),
        "{table}"
    );
}

#[test]
fn a_rejected_row_reads_as_a_source_error_with_no_inputs() {
    let rejected = rejected_row(100, 117);
    let records = [rejected];
    let mut renderer = renderer_over(&records);
    let text = rendered_with_summary(&mut renderer, &records);
    let block = text
        .split_once("SOURCE ERROR (stage: thermometer, error journal: ")
        .expect("error heading")
        .1;
    let lines: Vec<_> = block.lines().collect();
    assert_eq!(lines[1], "runtime.source.record_rejected ← thermometer()");
    assert_eq!(lines[5], r#"  "kind": "Validation","#);
    assert_eq!(
        lines[6],
        r#"  "detail": "record 117: invalid record (line 119)","#
    );
    assert!(!block.contains("execution_type"));
}

#[test]
fn an_output_printed_before_its_input_names_the_input_type_it_recorded() {
    let outputs = (100..250).map(|n| fact(2, n + 1000, &[n], json!({ "n": n })));
    let inputs = (100..250).map(|n| fact(1, n, &[], json!({ "reading": n })));
    let records: Vec<_> = outputs.chain(inputs).collect();
    let mut renderer = renderer_over(&records);
    let text = rendered_with_summary(&mut renderer, &records);
    let first_output = text.find("TRANSFORM (stage: classify").expect("an output");
    let first_input = text.find("SOURCE (stage: thermometer").expect("an input");
    assert!(first_output < first_input, "{text}");
    assert_eq!(
        text.matches("sensor.classified ← classify(sensor.reading)")
            .count(),
        150,
        "{text}"
    );
    assert!(!text.contains("input not recorded"), "{text}");
}

#[test]
fn the_summary_counts_each_record_once_with_errors_by_kind() {
    use obzenflow_core::event::payloads::effect_payload::EffectRecord;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{SinkOperationFailed, SinkOperationPhase, SinkWritePhase};

    let first = fact(1, 100, &[], json!({"id": 1}));
    let second = fact(1, 101, &[], json!({"id": 2}));
    let third = fact(1, 102, &[], json!({"id": 3}));
    let classified = fact(2, 103, &[100], json!({"id": 1}));
    let failed_call: EffectRecord = serde_json::from_value(json!({
        "cursor": {"recorded_flow_id": id(10), "stage_key": "classify", "input_seq": 3, "effect_ordinal": 0},
        "descriptor_hash": "fixture",
        "descriptor": {
            "effect_type": "sensor.calibrate", "label": "calibrate", "schema_version": 1,
            "stage_logic_version": "v1", "canonical_input_hash": "fixture", "binding": {"mode": "portless"}
        },
        "observation": {"observation": "execution_failed", "authority": "port_return", "external_completion": "unknown"},
        "outcome": {"outcome": "failed", "error_type": "timeout", "error_message": "calibration unavailable", "retry": "retryable"}
    }))
    .unwrap();
    let effect = execution(104, &[102], ExecutionPayload::EffectRecord(failed_call));
    let rejected = processing_failed(2, 105, 101, "reading 2 has no temperature");
    let classified_three = fact(2, 106, &[102], json!({"id": 3}));
    let buffered = receipt(3, 107, &classified, json!({"result": "buffered"}));
    let delivered = receipt(3, 108, &classified, json!({"result": "success"}));
    let failed_receipt = receipt(
        3,
        109,
        &classified_three,
        json!({"result": "failed", "error_type": "sink_write_current_only_failed", "error_message": "unavailable"}),
    );
    let sink_stage: obzenflow_core::StageId = serde_json::from_value(json!(id(3))).unwrap();
    let operation = in_error_journal(
        on_stage(
            fact(3, 110, &[109], json!({})),
            3,
            "written",
            StageType::Sink,
        ),
        ExecutionPayload::SinkOperationFailed(SinkOperationFailed {
            stage_id: sink_stage,
            stage_key: "written".into(),
            logical_destination: "written".into(),
            causal_event_id: Some(serde_json::from_value(json!(id(106))).unwrap()),
            input_position: Some(2),
            failed_delivery_event_id: Some(serde_json::from_value(json!(id(109))).unwrap()),
            operation_subject_event_id: None,
            phase: SinkOperationPhase::Write(SinkWritePhase::Execute),
            kind: ErrorKind::Remote,
            destination_error_code: None,
            detail: "unavailable".into(),
        }),
    );
    let mut write_failed = processing_failed(3, 111, 106, "unavailable");
    write_failed = on_stage(write_failed, 3, "written", StageType::Sink);
    write_failed.journal = operation.journal.clone();
    if let RunRecordData::Chain(row) = &mut write_failed.record {
        if let ChainPayload::Execution(ExecutionPayload::StageProcessingFailed(failure)) =
            &mut row.payload
        {
            failure.kind = ErrorKind::Remote;
        }
        row.envelope.provenance.event.processing.status = ProcessingStatus::Error {
            message: "unavailable".into(),
            kind: Some(ErrorKind::Remote),
        };
    }
    let records = [
        first,
        second,
        third,
        classified,
        effect,
        rejected,
        classified_three,
        buffered,
        delivered,
        failed_receipt,
        operation,
        write_failed,
    ];
    let mut renderer = renderer_over(&records);
    let text = rendered_with_summary(&mut renderer, &records);
    let table = text.split_once("\nJOURNALS\n").unwrap().1;
    let rows: Vec<_> = table
        .lines()
        .skip_while(|line| !line.trim_start().starts_with("Stage"))
        .take(4)
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect();
    assert_eq!(
        rows,
        [
            "Stage Data records Error records",
            "thermometer 3 0",
            "classify 2 1 Validation",
            "written 1 1 Remote",
        ]
    );
    assert!(text.contains("\nRecords in an error journal were delivered but not processed.\n"));
    assert!(!text.contains("Journal counts include every observed entry"));
    assert!(text.contains("SINK ERROR (stage: written, error journal: "));
}

#[test]
fn an_earlier_runs_rejection_and_its_repeat_are_listed_and_counted_once() {
    let mut killed_rejection = rejected_row(100, 164);
    killed_rejection.run = RunIdentity {
        flow_id: serde_json::from_value(json!(id(20))).unwrap(),
        pipeline_writer_id: killed_rejection.run.pipeline_writer_id,
    };
    killed_rejection.journal.id = serde_json::from_value(json!(id(35))).unwrap();
    let repeated = || Listed {
        record: rejected_row(101, 164),
        repeat_of: Some(RecordedAt::of(&killed_rejection)),
    };

    let repeat = rejected_row(101, 164);
    let records = [repeat.clone()];
    let mut renderer = renderer_over(&records);
    renderer.begin(&mut Vec::new(), &repeat.run, false).unwrap();
    let mut output = Vec::new();
    renderer
        .record(&mut output, killed_rejection.clone())
        .unwrap();
    renderer.record(&mut output, repeated()).unwrap();
    let text = String::from_utf8(output).unwrap() + &summary_text(&mut renderer, &records);
    let killed = format!("flow_{}", id(20));
    assert_eq!(
        text.matches("runtime.source.record_rejected ← thermometer()")
            .count(),
        2
    );
    assert!(
        text.contains(&format!("\nrecorded by run {killed}\n")),
        "{text}"
    );
    assert!(text.contains(&format!(
        "\nrejected again, first recorded by run {killed}\n"
    )));
    let table = text.split_once("\nJOURNALS\n").unwrap().1;
    assert!(table.contains("\n  A row rejected again after a crash counts once.\n"));
    let thermometer = table
        .lines()
        .find(|line| line.trim_start().starts_with("thermometer"))
        .unwrap();
    assert!(
        thermometer.trim_end().ends_with("1 Validation"),
        "{thermometer}"
    );

    let mut jsonl = Renderer::new(
        &ViewArgs {
            jsonl: true,
            ..ViewArgs::default()
        },
        false,
        false,
        records.iter().map(|record| &record.journal),
    );
    jsonl.begin(&mut Vec::new(), &repeat.run, false).unwrap();
    let mut output = Vec::new();
    jsonl.record(&mut output, killed_rejection.clone()).unwrap();
    jsonl.record(&mut output, repeated()).unwrap();
    let lines: Vec<Value> = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["run"]["flow_id"], json!(id(20)));
    assert!(lines[0].get("repeat_of").is_none());
    assert_eq!(
        lines[1]["repeat_of"],
        json!({
            "run": lines[0]["run"],
            "journal": lines[0]["journal"],
            "position": lines[0]["position"]
        })
    );
}
