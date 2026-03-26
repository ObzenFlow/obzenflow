// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::Arc;

use obzenflow_core::event::types::{Count, DurationMs, SeqNo, ViolationCause};
use obzenflow_core::event::{ChainEvent, ChainEventContent, ChainEventFactory};
use obzenflow_core::journal::Journal;
use obzenflow_core::{JournalOwner, NoControlMiddleware, StageId, WriterId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use obzenflow_runtime::messaging::upstream_subscription::{
    ContractConfig, ContractsWiring, ReaderProgress, UpstreamSubscription,
};
use obzenflow_runtime::messaging::PollResult;
use ulid::Ulid;

/// Helper to create a simple data event for a given writer and seq
fn make_data_event(writer: WriterId, seq: u64) -> ChainEvent {
    let payload = serde_json::json!({ "seq": seq });
    ChainEventFactory::data_event(writer, "test_event", payload)
}

/// Helper to create a simple EOF event for a given writer
fn make_eof_event(writer: WriterId, seq: u64) -> ChainEvent {
    let mut eof = ChainEventFactory::eof_event(writer, true);
    if let ChainEventContent::FlowControl(
        obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::Eof {
            writer_id,
            writer_seq,
            ..
        },
    ) = &mut eof.content
    {
        *writer_id = Some(writer);
        *writer_seq = Some(SeqNo(seq));
    }
    eof
}

/// Build a subscription over a single journal and attach contracts.
async fn build_subscription_with_contracts(
    writer_id: WriterId,
) -> (
    UpstreamSubscription<ChainEvent>,
    Arc<MemoryJournal<ChainEvent>>,
    Arc<DiskJournal<ChainEvent>>,
    StageId,
) {
    // Contract events can use an in-memory journal
    let data_journal: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::with_owner(
        JournalOwner::stage(StageId::new()),
    ));

    // Upstream events need a disk journal so the subscription can obtain a reader
    let base = std::env::temp_dir().join(format!(
        "contract_state_alignment_{}",
        Ulid::new().to_string()
    ));
    std::fs::create_dir_all(&base).expect("failed to create temp dir for disk journal");
    let upstream_path = base.join("upstream.log");
    let upstream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(upstream_path, JournalOwner::stage(StageId::new()))
            .expect("failed to create disk journal"),
    );

    let stage_id = StageId::new();

    let mut subscription = UpstreamSubscription::new_with_names(
        "contract_test",
        &[(
            stage_id,
            "upstream".into(),
            upstream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
    )
    .await
    .expect("failed to create subscription");

    let config = ContractConfig {
        progress_min_events: Count(1),
        progress_max_interval: DurationMs(1_000),
        stall_threshold: DurationMs(5_000),
        stall_cooloff: DurationMs(0),
        stall_checks_before_emit: 1,
    };

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id,
        contract_journal: data_journal.clone(),
        config,
        system_journal: None,
        reader_stage: Some(stage_id),
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    (subscription, data_journal, upstream_journal, stage_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn contract_state_tracks_seq_and_emits_final() {
    // Writer for the consuming stage
    let consumer_stage = StageId::new();
    let consumer_writer = WriterId::from(consumer_stage);
    let (mut subscription, contract_journal, upstream_journal, upstream_stage) =
        build_subscription_with_contracts(consumer_writer).await;

    // FSM-owned contract state for a single reader (upstream side)
    let mut progress = [ReaderProgress::new(upstream_stage)];

    let upstream_writer = WriterId::from(upstream_stage);

    // Append two data events then EOF to the upstream journal
    upstream_journal
        .append(make_data_event(upstream_writer, 1), None)
        .await
        .expect("append data 1");
    upstream_journal
        .append(make_data_event(upstream_writer, 2), None)
        .await
        .expect("append data 2");
    upstream_journal
        .append(make_eof_event(upstream_writer, 2), None)
        .await
        .expect("append eof");

    // Consume events through the subscription, updating FSM-owned contract state
    loop {
        match subscription
            .poll_next_with_state("test", Some(&mut progress[..]))
            .await
        {
            PollResult::Event(_env) => {
                // keep looping until EOF has been consumed
                continue;
            }
            PollResult::NoEvents => break,
            PollResult::Error(e) => {
                panic!("subscription error: {e}");
            }
        }
    }

    // At this point, the reader has seen the two data events and EOF,
    // and FSM-owned ReaderProgress should reflect the two data events.
    assert_eq!(
        progress[0].reader_seq,
        SeqNo(2),
        "FSM-owned ReaderProgress should track data seq"
    );

    // Run contract checks; this should emit a consumption_final event.
    let status = subscription.check_contracts(&mut progress[..]).await;
    match status {
        obzenflow_runtime::messaging::upstream_subscription::ContractStatus::ProgressEmitted
        | obzenflow_runtime::messaging::upstream_subscription::ContractStatus::Healthy => {
            // OK – final emission is part of progress path
        }
        other => panic!("unexpected contract status: {other:?}"),
    }

    // Inspect the contract journal and ensure we see a ConsumptionFinal event
    let events: Vec<_> = contract_journal
        .read_causally_ordered()
        .await
        .expect("read contract events");
    let final_events: Vec<_> = events
        .iter()
        .filter(|env| matches!(
            env.event.content,
            ChainEventContent::FlowControl(
                obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionFinal { .. }
            )
        ))
        .collect();

    assert!(
        !final_events.is_empty(),
        "expected at least one ConsumptionFinal event in contract journal"
    );

    // The consumed_count in the final event should match FSM-owned reader_seq.
    if let ChainEventContent::FlowControl(
        obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionFinal {
            consumed_count,
            ..
        },
    ) = &final_events[0].event.content
    {
        assert_eq!(
            *consumed_count,
            Count(progress[0].reader_seq.0),
            "final event should report the same count as FSM-owned ReaderProgress"
        );
    } else {
        panic!("expected ConsumptionFinal payload in final contract event");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn contract_seq_divergence_missing_events_emits_gap_and_violation() {
    let consumer_stage = StageId::new();
    let consumer_writer = WriterId::from(consumer_stage);
    let (mut subscription, contract_journal, upstream_journal, upstream_stage) =
        build_subscription_with_contracts(consumer_writer).await;

    let mut progress = [ReaderProgress::new(upstream_stage)];

    let upstream_writer = WriterId::from(upstream_stage);

    // Append two data events, but EOF advertises 3 events (missing one).
    upstream_journal
        .append(make_data_event(upstream_writer, 1), None)
        .await
        .expect("append data 1");
    upstream_journal
        .append(make_data_event(upstream_writer, 2), None)
        .await
        .expect("append data 2");
    upstream_journal
        .append(make_eof_event(upstream_writer, 3), None)
        .await
        .expect("append eof");

    // Consume events through the subscription
    loop {
        match subscription
            .poll_next_with_state("test_missing", Some(&mut progress[..]))
            .await
        {
            PollResult::Event(_env) => continue,
            PollResult::NoEvents => break,
            PollResult::Error(e) => panic!("subscription error: {e}"),
        }
    }

    // Reader saw 2 data events; advertised seq from EOF is 3.
    assert_eq!(progress[0].reader_seq, SeqNo(2));
    assert_eq!(progress[0].advertised_writer_seq, Some(SeqNo(3)));

    let status = subscription.check_contracts(&mut progress[..]).await;

    // Status should indicate a violation with SeqDivergence.
    match status {
        obzenflow_runtime::messaging::upstream_subscription::ContractStatus::Violated {
            upstream,
            cause,
        } => {
            assert_eq!(upstream, upstream_stage);
            match cause {
                ViolationCause::SeqDivergence { advertised, reader } => {
                    assert_eq!(advertised, Some(SeqNo(3)));
                    assert_eq!(reader, SeqNo(2));
                }
                other => panic!("unexpected violation cause: {other:?}"),
            }
        }
        other => panic!("expected ContractStatus::Violated, got {other:?}"),
    }

    assert!(
        progress[0].contract_violated,
        "progress should be marked violated"
    );
    assert!(
        progress[0].final_emitted,
        "progress should be marked final_emitted"
    );

    // Inspect contract journal for gap + final events
    let events: Vec<_> = contract_journal
        .read_causally_ordered()
        .await
        .expect("read contract events");

    let gap_events: Vec<_> = events
        .iter()
        .filter(|env| {
            matches!(
                env.event.content,
                ChainEventContent::FlowControl(
                    obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionGap { .. }
                )
            )
        })
        .collect();
    assert!(
        !gap_events.is_empty(),
        "expected a ConsumptionGap event for missing events"
    );

    if let ChainEventContent::FlowControl(
        obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionGap {
            from_seq,
            to_seq,
            upstream,
        },
    ) = &gap_events[0].event.content
    {
        assert_eq!(*from_seq, SeqNo(3));
        assert_eq!(*to_seq, SeqNo(3));
        assert_eq!(*upstream, upstream_stage);
    } else {
        panic!("expected ConsumptionGap payload in gap event");
    }

    let final_events: Vec<_> = events
        .iter()
        .filter(|env| {
            matches!(
                env.event.content,
                ChainEventContent::FlowControl(
                    obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionFinal { .. }
                )
            )
        })
        .collect();
    assert!(
        !final_events.is_empty(),
        "expected a ConsumptionFinal event for missing-events violation"
    );

    if let ChainEventContent::FlowControl(
        obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionFinal {
            pass,
            consumed_count,
            expected_count,
            reader_seq,
            advertised_writer_seq,
            failure_reason,
            ..
        },
    ) = &final_events[0].event.content
    {
        assert!(!*pass, "final event should mark pass=false");
        assert_eq!(
            *consumed_count,
            Count(progress[0].reader_seq.0),
            "consumed_count should match reader_seq"
        );
        assert!(
            expected_count.is_none(),
            "expected_count is unused in this path"
        );
        assert_eq!(*reader_seq, SeqNo(2));
        assert_eq!(*advertised_writer_seq, Some(SeqNo(3)));

        match failure_reason {
            Some(ViolationCause::SeqDivergence { advertised, reader }) => {
                assert_eq!(*advertised, Some(SeqNo(3)));
                assert_eq!(*reader, SeqNo(2));
            }
            other => panic!("unexpected failure_reason: {other:?}"),
        }
    } else {
        panic!("expected ConsumptionFinal payload in final event");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn contract_seq_divergence_overconsumption_sets_violation_without_gap() {
    let consumer_stage = StageId::new();
    let consumer_writer = WriterId::from(consumer_stage);
    let (mut subscription, contract_journal, upstream_journal, upstream_stage) =
        build_subscription_with_contracts(consumer_writer).await;

    let mut progress = [ReaderProgress::new(upstream_stage)];

    let upstream_writer = WriterId::from(upstream_stage);

    // Upstream writes 2 data events but EOF only advertises 1.
    // This simulates an over-consumption divergence where the reader
    // has observed more data events than the upstream claims via EOF.
    upstream_journal
        .append(make_data_event(upstream_writer, 1), None)
        .await
        .expect("append data 1");
    upstream_journal
        .append(make_data_event(upstream_writer, 2), None)
        .await
        .expect("append data 2");
    upstream_journal
        .append(make_eof_event(upstream_writer, 1), None)
        .await
        .expect("append eof");

    // Consume events through the subscription
    loop {
        match subscription
            .poll_next_with_state("test_over", Some(&mut progress[..]))
            .await
        {
            PollResult::Event(_env) => continue,
            PollResult::NoEvents => break,
            PollResult::Error(e) => panic!("subscription error: {e}"),
        }
    }

    assert_eq!(progress[0].reader_seq, SeqNo(2));
    assert_eq!(progress[0].advertised_writer_seq, Some(SeqNo(1)));

    let status = subscription.check_contracts(&mut progress[..]).await;

    // Any SeqDivergence (including over-consumption) is now surfaced as a violation.
    match status {
        obzenflow_runtime::messaging::upstream_subscription::ContractStatus::Violated {
            upstream,
            cause,
        } => {
            assert_eq!(upstream, upstream_stage);
            match cause {
                ViolationCause::SeqDivergence { advertised, reader } => {
                    assert_eq!(advertised, Some(SeqNo(1)));
                    assert_eq!(reader, SeqNo(2));
                }
                other => panic!("unexpected violation cause: {other:?}"),
            }
        }
        other => {
            panic!("expected ContractStatus::Violated for over-consumption path, got {other:?}")
        }
    }

    assert!(
        progress[0].contract_violated,
        "progress should be marked violated"
    );
    assert!(
        progress[0].final_emitted,
        "progress should be marked final_emitted"
    );

    let events: Vec<_> = contract_journal
        .read_causally_ordered()
        .await
        .expect("read contract events");

    // Over-consumption should not emit a ConsumptionGap event.
    let gap_events: Vec<_> = events
        .iter()
        .filter(|env| {
            matches!(
                env.event.content,
                ChainEventContent::FlowControl(
                    obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionGap { .. }
                )
            )
        })
        .collect();
    assert!(
        gap_events.is_empty(),
        "did not expect ConsumptionGap for over-consumption divergence"
    );

    let final_events: Vec<_> = events
        .iter()
        .filter(|env| {
            matches!(
                env.event.content,
                ChainEventContent::FlowControl(
                    obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionFinal { .. }
                )
            )
        })
        .collect();
    assert!(
        !final_events.is_empty(),
        "expected a ConsumptionFinal event for over-consumption divergence"
    );

    if let ChainEventContent::FlowControl(
        obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::ConsumptionFinal {
            pass,
            consumed_count,
            reader_seq,
            advertised_writer_seq,
            failure_reason,
            ..
        },
    ) = &final_events[0].event.content
    {
        assert!(!*pass, "final event should mark pass=false");
        assert_eq!(*consumed_count, Count(2));
        assert_eq!(*reader_seq, SeqNo(2));
        assert_eq!(*advertised_writer_seq, Some(SeqNo(1)));

        match failure_reason {
            Some(ViolationCause::SeqDivergence { advertised, reader }) => {
                assert_eq!(*advertised, Some(SeqNo(1)));
                assert_eq!(*reader, SeqNo(2));
            }
            other => panic!("unexpected failure_reason: {other:?}"),
        }
    } else {
        panic!("expected ConsumptionFinal payload in final event");
    }
}
