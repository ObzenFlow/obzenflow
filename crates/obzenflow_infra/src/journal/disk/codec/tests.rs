// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::provenance::{JournalProvenance, RuntimeProvenance};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEvent, ChainEventFactory, ChainPayload};
use obzenflow_core::{EventId, JournalWriterId, StageId, WriterId};
use serde_json::{json, Value};
use std::io::Write;

fn record() -> JournalRecord<ChainPayload> {
    let mut event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "application.value",
        json!({
            "zero": -0.0, "integer": u64::MAX, "tiny": f64::MIN_POSITIVE,
            "nested": {"envelope": {"provenance": [null, true, "🙂\n\u{0}"]}},
            "float": f64::from_bits(0x3feb_0e70_04ce_3bfa),
        }),
    );
    event.runtime = Some(RuntimeProvenance::default());
    JournalRecord::commit_event(
        event,
        JournalProvenance {
            journal_writer_id: JournalWriterId::new(),
            vector_clock: VectorClock::new(),
            timestamp: "2037-01-02T03:04:05.123456789Z".parse().unwrap(),
            journal_group_id: None,
            journal_group_member: None,
        },
    )
    .unwrap()
}

fn persist(
    path: &Path,
    record: &JournalRecord<ChainPayload>,
    store: DefinitionStore,
) -> (u64, Vec<u8>) {
    let prepared = prepare(std::slice::from_ref(record), None, path, store).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    let offset = file.metadata().unwrap().len();
    file.write_all(&prepared.bytes).unwrap();
    file.flush().unwrap();
    let bytes = prepared.bytes.clone();
    prepared.commit(offset);
    (offset, bytes)
}

fn decode(path: &Path, offset: u64, bytes: &[u8]) -> Result<JournalRecord<ChainPayload>> {
    let body = frame::validate(bytes).map_err(frame::io_error)?;
    Ok(Decoder::cold(path)
        .decode::<ChainEvent>(body, offset)?
        .into_records()
        .remove(0))
}

#[test]
fn ordinary_and_measured_decode_share_validation_and_record_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("decode-modes.log");
    let store = DefinitionStore::default();
    let original = complete_observation_record();
    for _ in 0..2 {
        // The second frame exercises external definition references as well.
        let (offset, bytes) = persist(&path, &original, store.clone());
        let body = frame::validate(&bytes).unwrap();
        let ordinary = Decoder::cold(&path)
            .decode::<ChainEvent>(body, offset)
            .unwrap();
        let (measured, sizes) = Decoder::cold(&path)
            .decode_measured::<ChainEvent>(body, offset)
            .unwrap();
        assert_eq!(
            serde_json::to_value(ordinary.into_records()).unwrap(),
            serde_json::to_value(measured.into_records()).unwrap()
        );
        assert_eq!(sizes.records, 1);
        assert_eq!(sizes.packets, 1);
        assert_eq!(
            sizes.provenance + sizes.observability + sizes.payload + sizes.shared,
            bytes.len()
        );
        for malformed in [&body[..body.len() - 1], &[body, &[0]].concat()] {
            assert!(Decoder::cold(&path)
                .decode::<ChainEvent>(malformed, offset)
                .is_err());
            assert!(Decoder::cold(&path)
                .decode_measured::<ChainEvent>(malformed, offset)
                .is_err());
        }
    }
}

#[test]
fn absolute_current_numbers_do_not_depend_on_previous_numeric_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("numbers.log");
    let store = DefinitionStore::default();
    let mut original = record();
    persist(&path, &original, store.clone());
    let accounting = &mut original
        .envelope
        .provenance
        .event
        .runtime
        .as_mut()
        .unwrap()
        .accounting;
    accounting.events_processed_total = 1000;
    accounting.events_emitted_total = 1000;
    let (previous_offset, previous) = persist(&path, &original, store.clone());
    let accounting = &mut original
        .envelope
        .provenance
        .event
        .runtime
        .as_mut()
        .unwrap()
        .accounting;
    accounting.events_processed_total = 1001;
    accounting.events_emitted_total = 1001;
    let (offset, current) = persist(&path, &original, store.clone());
    let mut disk = std::fs::read(&path).unwrap();
    // Damage the preceding numeric record, leaving the immutable definition
    // carrier intact. An addressed read must apply zero preceding updates.
    disk[previous_offset as usize + previous.len() - frame::TRAILER_LEN - 1] ^= 1;
    std::fs::write(&path, disk).unwrap();
    let restored = decode(&path, offset, &current).unwrap();
    assert_eq!(
        serde_json::to_vec(&restored).unwrap(),
        serde_json::to_vec(&original).unwrap()
    );
    let accounting = &restored
        .envelope
        .provenance
        .event
        .runtime
        .as_ref()
        .unwrap()
        .accounting;
    assert_eq!(accounting.events_processed_total, 1001);
    assert_eq!(accounting.events_emitted_total, 1001);
    for total in [1001, 999, 0, u64::MAX] {
        let accounting = &mut original
            .envelope
            .provenance
            .event
            .runtime
            .as_mut()
            .unwrap()
            .accounting;
        accounting.events_processed_total = total;
        accounting.events_emitted_total = total;
        original.envelope.provenance.journal.timestamp =
            "1969-12-31T23:59:59.000000001Z".parse().unwrap();
        let (offset, bytes) = persist(&path, &original, store.clone());
        assert_eq!(
            serde_json::to_vec(&decode(&path, offset, &bytes).unwrap()).unwrap(),
            serde_json::to_vec(&original).unwrap()
        );
    }
}

#[test]
fn origin_metadata_keeps_application_keys_and_values_opaque() {
    use super::values::Standalone;
    let source = test_data::record(test_data::Stage::Source, 0);
    let mut origin = serde_json::to_value(
        source
            .envelope
            .provenance
            .event
            .correlation
            .unwrap()
            .payload
            .unwrap(),
    )
    .unwrap();
    origin["metadata"] = json!({
        "source_event_id": origin["entry_event_id"],
        "flow_id": "application-flow",
        "flow_name": "application-name",
    });
    for change in 0..6 {
        let mut value = origin.clone();
        match change {
            1 => value["metadata"]["source_event_id"] = json!(EventId::new()),
            2 => value["metadata"]["custom"] = json!({"source_event_id": null, "zero": -0.0}),
            3 => value["metadata"]["flow_id"] = Value::Null,
            4 => value["metadata"] = Value::Null,
            5 => {
                value.as_object_mut().unwrap().remove("metadata");
            }
            _ => {}
        }
        let mut bytes = Vec::new();
        values::write(
            Kind::Struct(Layout::Origin),
            &value,
            &mut bytes,
            &mut Standalone,
        )
        .unwrap();
        let mut cursor = Cursor::new(&bytes);
        let decoded =
            values::read(Kind::Struct(Layout::Origin), &mut cursor, &mut Standalone).unwrap();
        cursor.finish().unwrap();
        assert_eq!(
            serde_json::to_vec(&value).unwrap(),
            serde_json::to_vec(&decoded).unwrap()
        );
    }
}

#[test]
fn optional_custom_json_preserves_missing_null_and_empty_through_disk_and_jsonl() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata.log");
    let store = DefinitionStore::default();
    let records = [
        test_data::record(test_data::Stage::Source, 0),
        test_data::record(test_data::Stage::Receipt, 0),
    ];
    for metadata in [
        None,
        Some(Value::Null),
        Some(json!({})),
        Some(json!([])),
        Some(json!({"n":-0.0})),
    ] {
        for mut record in records.clone() {
            record
                .envelope
                .provenance
                .event
                .correlation
                .as_mut()
                .unwrap()
                .payload
                .as_mut()
                .unwrap()
                .metadata = metadata.clone();
            if let ChainPayload::Delivery(delivery) = &mut record.payload {
                delivery.middleware_context = metadata.clone();
            }
            let original = serde_json::to_vec(&serde_json::to_value(&record).unwrap()).unwrap();
            let (offset, bytes) = persist(&path, &record, store.clone());
            let restored = decode(&path, offset, &bytes).unwrap();
            let jsonl = serde_json::to_vec(&serde_json::to_value(&restored).unwrap()).unwrap();
            assert_eq!(original, jsonl);
            let restored: JournalRecord<ChainPayload> = serde_json::from_slice(&jsonl).unwrap();
            assert_eq!(
                original,
                serde_json::to_vec(&serde_json::to_value(restored).unwrap()).unwrap()
            );
        }
    }
}

#[test]
fn representative_records_preserve_all_fields_and_attribute_complete_origin_costs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("representative.log");
    let store = DefinitionStore::default();
    let mut provenance = 0;
    let mut inline_origins = 0;
    let mut references = 0;
    let mut frame_bytes = 0;
    let mut origin_count = 0;
    let mut families = std::collections::BTreeMap::<String, (usize, f64, usize)>::new();
    let records = test_data::records();
    assert_eq!(records.len(), 3 * test_data::RECORDS_PER_STAGE);
    for original in &records {
        let (offset, bytes) = persist(&path, original, store.clone());
        let (frame, sizes) = Decoder::cold(&path)
            .decode_measured::<ChainEvent>(frame::validate(&bytes).unwrap(), offset)
            .unwrap();
        let restored = frame.into_records().remove(0);
        assert_eq!(
            serde_json::to_vec(&serde_json::to_value(original).unwrap()).unwrap(),
            serde_json::to_vec(&serde_json::to_value(restored).unwrap()).unwrap()
        );
        provenance += sizes.provenance;
        inline_origins += sizes.inline_definition_bytes[2];
        references += sizes.definition_reference_bytes;
        frame_bytes += bytes.len();
        origin_count += sizes.inline_definition_counts[2];
        let family = families
            .entry(
                original
                    .envelope
                    .provenance
                    .event
                    .flow_context
                    .stage_name
                    .clone(),
            )
            .or_default();
        family.0 += 1;
        family.1 += sizes.provenance as f64 + sizes.shared as f64 / 2.0;
        family.2 += sizes.inline_definition_counts[2];
    }
    // The first input has no successful transform or receipt; the downstream
    // samples extend one input beyond the source sample. All shared origins
    // must be references after their first committed definition.
    assert_eq!(origin_count, test_data::RECORDS_PER_STAGE + 1);
    assert!(references > 0);
    println!("Representative stream: {} records, {frame_bytes} frame bytes, {provenance} provenance, {inline_origins} complete inline origins, {references} definition references", records.len());
    println!("Origins: {origin_count}; stage counts/provenance bytes/inline origins: {families:?}");
}

#[test]
fn provenance_growth_follows_retained_relationships_without_expanding_ancestors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("growth.log");
    let store = DefinitionStore::default();
    let mut original = record();
    let mut previous = original.envelope.provenance.event.id;
    let mut maxima = [0; 3];
    for (bucket, count) in [16, 128, 1024].into_iter().enumerate() {
        for index in 0..count {
            original.envelope.provenance.event.id = EventId::new();
            original.envelope.provenance.event.causality.parent_ids = vec![previous];
            original
                .envelope
                .provenance
                .event
                .runtime
                .as_mut()
                .unwrap()
                .accounting
                .events_processed_total = index;
            let (_, bytes) = persist(&path, &original, store.clone());
            if index > 0 {
                maxima[bucket] = maxima[bucket].max(bytes.len());
            }
            previous = original.envelope.provenance.event.id;
        }
    }
    assert!(
        maxima[2] <= maxima[0] + 8,
        "only absolute integer/address widths may grow: {maxima:?}"
    );
    let mut prior = None;
    for count in [1usize, 100, 1000] {
        original.envelope.provenance.event.causality.parent_ids =
            (0..count).map(|_| EventId::new()).collect();
        let (offset, bytes) = persist(&path, &original, store.clone());
        let restored = decode(&path, offset, &bytes).unwrap();
        assert_eq!(
            restored.envelope.provenance.event.causality.parent_ids,
            original.envelope.provenance.event.causality.parent_ids
        );
        if let Some((prior_count, prior_bytes)) = prior {
            assert!(bytes.len() - prior_bytes <= (count - prior_count) * 16 + 8);
        }
        prior = Some((count, bytes.len()));
    }
}

#[test]
fn immutable_references_are_committed_run_local_complete_and_not_keyed_by_event_id() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.log");
    let target = dir.path().join("target.log");
    let store = DefinitionStore::default();
    let original = record();
    let (_, carrier) = persist(&source, &original, store.clone());
    let mut forwarded = original.clone();
    // Deliberately retain EventId while changing the complete authored context.
    forwarded.envelope.provenance.event.flow_context.stage_name = "different context".into();
    let (offset, child) = persist(&target, &forwarded, store.clone());
    assert_eq!(
        serde_json::to_vec(&decode(&target, offset, &child).unwrap()).unwrap(),
        serde_json::to_vec(&forwarded).unwrap()
    );
    assert!(child.len() < carrier.len() + 32);
    for cut in [0, frame::HEADER_LEN, carrier.len() - 1] {
        std::fs::write(&source, &carrier[..cut]).unwrap();
        assert!(
            decode(&target, offset, &child).is_err(),
            "uncommitted carrier at {cut}"
        );
    }
    let mut corrupt = carrier.clone();
    corrupt[frame::HEADER_LEN + 3] ^= 1;
    std::fs::write(&source, corrupt).unwrap();
    assert!(decode(&target, offset, &child).is_err());
    std::fs::write(&source, &carrier).unwrap();
    std::fs::remove_file(&source).unwrap();
    assert!(decode(&target, offset, &child).is_err());

    let independent = DefinitionStore::default();
    let abandoned = prepare(
        std::slice::from_ref(&original),
        None,
        &source,
        independent.clone(),
    )
    .unwrap();
    drop(abandoned);
    let subsequent = prepare(std::slice::from_ref(&original), None, &target, independent).unwrap();
    assert!(
        decode(&target, 0, &subsequent.bytes).is_ok(),
        "aborted preparations publish no references"
    );
}

fn complete_observation_record() -> JournalRecord<ChainPayload> {
    let mut value = serde_json::to_value(record()).unwrap();
    let author = value["envelope"]["provenance"]["event"]["writer_id"].clone();
    let observer = serde_json::to_value(WriterId::from(StageId::new())).unwrap();
    let parent = EventId::new();
    let event = &mut value["envelope"]["provenance"]["event"];
    event["causality"]["parent_ids"] = json!([parent]);
    event["correlation"] = json!({"ids":[parent, event["id"]], "truncated": false,
        "payload": {"entry_time_ns": u64::MAX, "entry_event_id": parent,
            "metadata": {"id": "opaque", "negative_zero": -0.0, "nested": [null, u64::MAX, {"event_type": "custom"}]}}});
    event["runtime"]["accounting"]["events_processed_total"] = json!(u64::MAX);
    event["runtime"]["accounting"]["events_emitted_total"] = json!(9_007_199_254_740_993u64);
    let capture = json!({"capture_scope":{"flow_id": obzenflow_core::FlowId::new(), "resume_generation": u64::MAX},
        "observer": observer, "capture_seq": u64::MAX, "capture_reason":"periodic", "observed_at_ms": u64::MAX});
    let mut snapshot_capture = capture.clone();
    snapshot_capture["capture_seq"] = json!(u64::MAX - 1);
    snapshot_capture["observer"] = author.clone();
    let clock = json!({"clocks":{"independent-writer": u64::MAX, "zero-writer":0}});
    value["envelope"]["provenance"]["journal"]["vector_clock"] = clock.clone();
    let mut different_clock = clock.clone();
    different_clock["clocks"]["independent-writer"] = json!(u64::MAX - 1);
    let packet = json!({
        "capture": capture,
        "processing_time": u64::MAX,
        "runtime": {"in_flight":0,"join_reference_since_last_stream": u64::MAX,"time_in_state_ms":1001,
            "event_loops_total":u64::MAX,"event_loops_with_work_total":0,
            "timing":{"processing_time_count":u64::MAX,"processing_time_sum_nanos":u64::MAX,
                "recent_p50_ms":0,"recent_p90_ms":null,"recent_p95_ms":1,"recent_p99_ms":u64::MAX,"recent_p999_ms":u64::MAX,
                "window":{"started_at_ms":0,"ended_at_ms":u64::MAX}},
            "circuit_breaker":{"observed_state":"half_open","requests_total":u64::MAX,"successes_total":0,"failures_total":1,"slow_total":2,"rejections_total":3,"opened_total":4,
                "time_closed_seconds":-0.0,"time_open_seconds":f64::MAX,"time_half_open_seconds":f64::MIN_POSITIVE},
            "rate_limiter":{"events_total":u64::MAX,"delayed_total":1,"tokens_consumed_total":-0.0,"delay_seconds_total":0.125,"bucket_tokens":f64::MIN_POSITIVE,"bucket_capacity":f64::MAX},
            "effect_circuit_breakers":[{"effect_type":"payments.authorise","cb_requests_total":u64::MAX,"cb_successes_total":1,"cb_failures_total":2,"cb_slow_total":3,"cb_rejections_total":4,"cb_opened_total":5,
                "cb_time_closed_seconds":-0.0,"cb_time_open_seconds":0.25,"cb_time_half_open_seconds":0.5,"cb_state":0.5}],
            "effect_rate_limiters":[{"effect_type":"payments.authorise","rl_events_total":u64::MAX,"rl_delayed_total":1001,"rl_tokens_consumed_total":-0.0,"rl_delay_seconds_total":f64::MIN_POSITIVE,"rl_bucket_tokens":12.5,"rl_bucket_capacity":100.0}]},
        "runtime_snapshot":{"capture":snapshot_capture,"fsm_state":"Running","progress":{"reader_seq":u64::MAX,"receipted_seq":1001,"writer_seq":1000,
            "last_consumed_event_id":parent,"last_consumed_writer":JournalWriterId::new(),"last_consumed_vector_clock":clock,
            "last_receipted_event_id":parent,"last_receipted_vector_clock":different_clock,"last_emitted_event_id":value["envelope"]["provenance"]["event"]["id"],"last_emitted_writer":author}},
        "metrics":{"events_processed":u64::MAX,"events_in_flight":u32::MAX,"queue_depth":0,"processing_rate":-0.0,"error_rate":0.0,"latency_p50_ms":f64::MIN_POSITIVE,"latency_p99_ms":f64::MAX},
        "sli":{"availability":1.0,"error_budget_remaining":-0.0,"latency_budget_used":0.125},
        "records":[
            {"observation_type":"llm","metadata":{"schema_version":1,"provider":"fixture","model":"complete-model","hashes":{"version":"sha256-v1","prompt_hash":"a","params_hash":"b","schema_hash":"c"},"usage":{"source":"provider","input_tokens":1001,"output_tokens":1000,"total_tokens":2001},"cache":{"mode":"replay","hit":true}}},
            {"observation_type":"circuit_breaker_summary","effect_type":"effect","window_duration_s":1,"requests_processed":2,"requests_rejected":3,"observed_state":"open","consecutive_failures":4,"rejection_rate":-0.0,"successes_total":5,"failures_total":6,"opened_total":7,"time_in_closed_seconds":0.0,"time_in_open_seconds":0.5,"time_in_half_open_seconds":0.25},
            {"observation_type":"rate_limiter_activity","effect_type":"effect","window_ms":1,"delayed_events":2,"delay_ms_total":3,"delay_ms_max":4,"limit_rate":-0.0},
            {"observation_type":"rate_limiter_utilisation","utilization_percent":0.25,"events_in_window":u64::MAX,"window_size_ms":0},
            {"observation_type":"backpressure_activity","window_ms":0,"delayed_events":1,"delay_ms_total":2,"delay_ms_max":3,"min_credit":0,"limiting_downstream_stage_id":StageId::new()},
            {"observation_type":"resource_usage","cpu_percent":-0.0,"memory_bytes":u64::MAX,"thread_count":null},
            {"observation_type":"http_pull","requests_total":u64::MAX,"responses_2xx":1,"responses_4xx":2,"responses_5xx":3,"rate_limited_total":4,"retries_total":5,"events_decoded_total":6,"wait_seconds_rate_limit":-0.0,"wait_seconds_poll_interval":0.25,"wait_seconds_backoff":0.5},
            {"observation_type":"ai_chunking_work","rerender_attempts_total":u64::MAX,"max_decomposition_depth_reached":u32::MAX,"budget_overhead_tokens":1001,"excluded_items":[0,127,128]},
            {"observation_type":"stage_heartbeat","activity":{"kind":"polling"},"handler_blocked_ms":0,"last_consumed_event_id":parent,"last_output_event_id":null},
            {"observation_type":"edge_liveness","upstream":StageId::new(),"reader":StageId::new(),"state":"recovered","idle_ms":u64::MAX,"last_reader_seq":0,"last_event_id":parent},
            {"observation_type":"http_surface","snapshot":{"routes":[{"surface_name":"api","method":"Get","path":"/items","status_class":"2xx","requests_total":u64::MAX,"request_duration_ms_total":1001,"request_bytes_total":0,"response_bytes_total":1000}]}}
        ]
    });
    value["envelope"]["observability"] = packet;
    serde_json::from_value(value).unwrap()
}

#[test]
fn every_observation_family_and_absolute_boundary_survives_full_record_roundtrip() {
    let record = complete_observation_record();
    assert_eq!(
        record
            .envelope
            .observability
            .as_ref()
            .unwrap()
            .records
            .len(),
        11
    );
    for observations in [true, false] {
        let mut original = record.clone();
        if !observations {
            original.envelope.observability = None;
        }
        let prepared = prepare(
            std::slice::from_ref(&original),
            None,
            Path::new("fixture.log"),
            DefinitionStore::default(),
        )
        .unwrap();
        let restored = decode(Path::new("fixture.log"), 0, &prepared.bytes).unwrap();
        assert_eq!(
            serde_json::to_vec(&original).unwrap(),
            serde_json::to_vec(&restored).unwrap()
        );
        let value = serde_json::to_value(restored).unwrap();
        assert_eq!(
            value["payload"]["zero"].as_f64().unwrap().to_bits(),
            (-0.0f64).to_bits()
        );
        if observations {
            let packet = &value["envelope"]["observability"];
            assert_ne!(packet["capture"], packet["runtime_snapshot"]["capture"]);
            let progress = &packet["runtime_snapshot"]["progress"];
            assert_eq!(
                progress["last_consumed_event_id"],
                progress["last_receipted_event_id"]
            );
            assert_ne!(
                progress["last_consumed_vector_clock"],
                progress["last_receipted_vector_clock"]
            );
        }
    }
}

#[test]
fn streamed_scalars_preserve_the_existing_wire_bytes_and_presence_states() {
    use super::layout::DefaultValue;

    fn check<T: serde::Serialize>(kind: Kind, value: T, default: Option<DefaultValue>) {
        let logical = serde_json::to_value(&value).unwrap();
        let mut expected = Vec::new();
        let expected_state = if logical.is_null() {
            1
        } else if default.is_some_and(|default| values::is_default(&logical, default)) {
            2
        } else {
            values::write(kind, &logical, &mut expected, &mut values::Standalone).unwrap();
            3
        };
        let mut actual = Vec::new();
        let state =
            serialize::write(kind, &value, default, &mut actual, &mut values::Standalone).unwrap();
        assert_eq!(
            (state, &actual),
            (expected_state, &expected),
            "{kind:?}: {logical}"
        );
        if state == 3 {
            let mut input = Cursor::new(&actual);
            let restored: Value =
                deserialize::read(kind, &mut input, &mut values::Standalone).unwrap();
            input.finish().unwrap();
            assert_eq!(
                serde_json::to_vec(&restored).unwrap(),
                serde_json::to_vec(&logical).unwrap()
            );
        }
    }

    for value in [0, 1, 127, 128, 1000, 1001, 90000, u64::MAX] {
        check(Kind::Unsigned, value, Some(DefaultValue::Zero));
        check(Kind::Unsigned, value, None);
    }
    check(Kind::Unsigned, None::<u64>, Some(DefaultValue::Zero));
    check(Kind::Unsigned, Some(0u64), Some(DefaultValue::Zero));
    check(Kind::Unsigned, u64::MAX as i128, None);
    for value in [
        0.0,
        -0.0,
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::NAN,
    ] {
        check(Kind::Float, value, Some(DefaultValue::FloatZero));
    }
    for value in [false, true] {
        check(Kind::Boolean, value, Some(DefaultValue::False));
    }
    for value in ["", "text", "🙂\n\0"] {
        check(Kind::Text, value, None);
    }
    check(Kind::Text, "default", Some(DefaultValue::Text("default")));
    check(Kind::Id, EventId::new(), None);
    check(Kind::Enum(&["first", "second"]), "second", None);
}

#[test]
fn primitive_extremes_and_presence_states_are_exact() {
    for value in [
        json!(u64::MAX),
        json!(i64::MIN),
        json!(-0.0),
        json!(0.0),
        json!(f64::MAX),
        json!(f64::from_bits(1)),
        json!("0"),
        Value::Null,
        json!({"dynamic": [0, null, false]}),
    ] {
        let mut bytes = Vec::new();
        values::write(Kind::Value, &value, &mut bytes, &mut values::Standalone).unwrap();
        let mut input = Cursor::new(&bytes);
        let restored = values::read(Kind::Value, &mut input, &mut values::Standalone).unwrap();
        input.finish().unwrap();
        assert_eq!(
            serde_json::to_vec(&value).unwrap(),
            serde_json::to_vec(&restored).unwrap()
        );
    }
    for value in [
        json!({}),
        json!({"in_flight":null}),
        json!({"in_flight":0}),
        json!({"in_flight":1}),
    ] {
        let mut bytes = Vec::new();
        values::write(
            Kind::Struct(Layout::Measurements),
            &value,
            &mut bytes,
            &mut values::Standalone,
        )
        .unwrap();
        let restored = values::read(
            Kind::Struct(Layout::Measurements),
            &mut Cursor::new(&bytes),
            &mut values::Standalone,
        )
        .unwrap();
        assert_eq!(restored, value);
    }
}

#[test]
fn warm_caches_cannot_hide_missing_or_edited_carriers_and_archives_are_relocatable() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.log");
    let target = dir.path().join("target.log");
    let store = DefinitionStore::default();
    let original = record();
    let (_, carrier) = persist(&source, &original, store.clone());
    let (offset, child) = persist(&target, &original, store.clone());
    let body = frame::validate(&child).unwrap();
    let mut warm = Decoder {
        path: target.clone(),
        store,
    };
    warm.decode::<ChainEvent>(body, offset).unwrap();
    let mut corrupt = carrier.clone();
    corrupt[frame::HEADER_LEN + 5] ^= 1;
    std::fs::write(&source, corrupt).unwrap();
    let file = std::fs::File::options().write(true).open(&source).unwrap();
    file.set_times(std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH))
        .unwrap();
    assert!(warm.decode::<ChainEvent>(body, offset).is_err());
    std::fs::remove_file(&source).unwrap();
    assert!(warm.decode::<ChainEvent>(body, offset).is_err());
    std::fs::write(&source, carrier).unwrap();
    let moved = tempfile::tempdir().unwrap();
    let destination = moved.path().join("relocated");
    std::fs::rename(dir.path(), &destination).unwrap();
    let restored = decode(&destination.join("target.log"), offset, &child).unwrap();
    assert_eq!(
        serde_json::to_value(restored).unwrap(),
        serde_json::to_value(original).unwrap()
    );
}

#[test]
fn current_schema_fixtures_preserve_bytes_and_logical_records() {
    for (name, json, bytes) in [
        (
            "observations",
            include_str!("fixtures/observations.json"),
            include_bytes!("fixtures/observations.frame").as_slice(),
        ),
        (
            "plain",
            include_str!("fixtures/plain.json"),
            include_bytes!("fixtures/plain.frame").as_slice(),
        ),
    ] {
        let record: JournalRecord<ChainPayload> = serde_json::from_str(json).unwrap();
        let prepared = prepare(
            std::slice::from_ref(&record),
            None,
            Path::new("fixture.log"),
            DefinitionStore::default(),
        )
        .unwrap();
        // Regenerate these current-schema fixtures after an intentional schema
        // bump using this existing test, never a legacy frame converter.
        if std::env::var_os("UPDATE_JOURNAL_FIXTURES").is_some() {
            let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("src/journal/disk/codec/fixtures/{name}.frame"));
            std::fs::write(fixture, &prepared.bytes).unwrap();
        } else {
            assert_eq!(prepared.bytes, bytes);
        }
        let bytes = prepared.bytes.as_slice();
        let restored = decode(Path::new("fixture.log"), 0, bytes).unwrap();
        assert_eq!(
            serde_json::to_vec(&record).unwrap(),
            serde_json::to_vec(&restored).unwrap()
        );
        if let Some(packet) = record.envelope.observability {
            let families =
                obzenflow_core::event::observability::observation_families(packet).unwrap();
            assert_eq!(families.len(), 25);
            let keys: std::collections::HashSet<_> = families.iter().map(|(key, _)| key).collect();
            assert_eq!(
                keys.len(),
                families.len(),
                "independent measurements must not share a retention key"
            );
        }
    }
}
