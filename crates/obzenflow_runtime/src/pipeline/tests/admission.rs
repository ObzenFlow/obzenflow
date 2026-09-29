// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ordinary journal guarantees and acknowledgement admission.
use super::support::{make_fsm_context, new_system_journal};
use crate::pipeline::fsm::{
    build_pipeline_fsm_with_initial, PipelineFsmEvent as E, PipelineFsmState as S,
};
use crate::stages::common::stage_handle::{StageAck, StageMilestone};
use obzenflow_core::event::SystemEventFactory;
use obzenflow_core::journal::factory::FlowJournalFactory;
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{StageId, SystemId};

pub async fn controlled_journal_preserves_causality_groups_and_live_readers(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    use super::support::ControlledJournal;
    use crate::testing::assert_happens_before;
    use obzenflow_core::Journal;

    let mut journals = make_journals();
    let inner = new_system_journal(&mut *journals, SystemId::new());
    let journal = ControlledJournal::new(inner.clone());
    assert_eq!(journal.id(), inner.id());
    assert_eq!(journal.owner(), inner.owner());

    let parent = inner
        .append(
            SystemEventFactory::new(SystemId::new()).pipeline_starting(),
            Default::default(),
        )
        .await
        .unwrap();
    let writer = SystemId::new();
    let child = journal
        .append(
            SystemEventFactory::new(writer).pipeline_starting(),
            AppendOptions::from_record(Some(&parent)).unwrap(),
        )
        .await
        .unwrap();
    assert_happens_before(&parent, &child).unwrap();
    let group = journal
        .append_group(
            "fixture.causal-group",
            vec![
                SystemEventFactory::new(writer).pipeline_starting(),
                SystemEventFactory::new(writer).pipeline_starting(),
            ],
            AppendOptions::from_record(Some(&child)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(group.len(), 2);
    assert_happens_before(&child, &group[0]).unwrap();
    assert_happens_before(&group[0], &group[1]).unwrap();
    for (index, row) in group.iter().enumerate() {
        assert_eq!(
            row.envelope.provenance.journal.journal_group_id.as_deref(),
            Some("fixture.causal-group")
        );
        let member = row
            .envelope
            .provenance
            .journal
            .journal_group_member
            .as_ref()
            .unwrap();
        assert_eq!(member.index as usize, index);
        assert_eq!(member.size, 2);
    }
    let found = journal
        .read_event(&child.envelope.provenance.event.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        found.envelope.provenance.journal.vector_clock,
        child.envelope.provenance.journal.vector_clock
    );
    let stored = inner.read_all_unordered().await.unwrap();
    let observed = journal.read_causally_ordered().await.unwrap();
    assert_eq!(
        stored
            .iter()
            .map(|row| row.envelope.provenance.event.id)
            .collect::<Vec<_>>(),
        observed
            .iter()
            .map(|row| row.envelope.provenance.event.id)
            .collect::<Vec<_>>()
    );

    let mut reader = journal.reader_from(1).await.unwrap();
    assert_eq!(reader.position(), 1);
    for expected in [&child, &group[0], &group[1]] {
        assert_eq!(
            reader
                .next()
                .await
                .unwrap()
                .unwrap()
                .envelope
                .provenance
                .event
                .id,
            expected.envelope.provenance.event.id
        );
    }
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.is_at_end());
    let later = inner
        .append(
            SystemEventFactory::new(writer).pipeline_starting(),
            AppendOptions::from_record(Some(&group[1])).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        reader
            .next()
            .await
            .unwrap()
            .unwrap()
            .envelope
            .provenance
            .event
            .id,
        later.envelope.provenance.event.id
    );
    assert_eq!(reader.position(), 5);
    let tail = journal.read_last_n(2).await.unwrap();
    assert_eq!(
        tail[0].envelope.provenance.event.id,
        later.envelope.provenance.event.id
    );
    assert_eq!(
        tail[1].envelope.provenance.event.id,
        group[1].envelope.provenance.event.id
    );
}

pub async fn initialization_requires_all_child_acknowledgements(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let mut ctx = make_fsm_context(make_journals);
    let ids = [StageId::new(), StageId::new()];
    ctx.outstanding_milestones = ids.map(Into::into).into();
    let mut fsm = build_pipeline_fsm_with_initial(S::InitializingStages);
    let ack = |stage_id, milestone| {
        E::ChildAcknowledged(StageAck {
            stage_id,
            milestone,
            snapshot: Default::default(),
        })
    };
    for event in [
        ack(ids[0], StageMilestone::Started),
        ack(StageId::new(), StageMilestone::Initialized),
    ] {
        assert!(fsm.handle(event, &mut ctx).await.unwrap().is_empty());
    }
    assert!(fsm.handle(E::PhaseSatisfied, &mut ctx).await.is_err());
    for id in ids {
        fsm.handle(ack(id, StageMilestone::Initialized), &mut ctx)
            .await
            .unwrap();
        // Repeated observation neither consumes another child's acknowledgement nor starts work.
        assert!(fsm
            .handle(ack(id, StageMilestone::Initialized), &mut ctx)
            .await
            .unwrap()
            .is_empty());
    }
    let actions = fsm.handle(E::PhaseSatisfied, &mut ctx).await.unwrap();
    assert_eq!(fsm.state(), &S::StartingConsumers);
    assert_eq!(actions.len(), 1);
}

pub async fn child_acknowledgement_carries_causality_to_parent_publication(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    use crate::supervised_base::publication::{self, PublicationScope};
    let mut ctx = make_fsm_context(make_journals);
    let id = StageId::new();
    let journal = ctx.system_journal.clone();
    let record = journal
        .append(
            SystemEventFactory::new(SystemId::new()).pipeline_starting(),
            Default::default(),
        )
        .await
        .unwrap();
    let frontier = obzenflow_core::event::CausalFrontier::from_record(&record).unwrap();
    ctx.outstanding_milestones.insert(id.into());
    let scope = PublicationScope::new();
    scope
        .enter(async {
            let mut fsm = build_pipeline_fsm_with_initial(S::StartingConsumers);
            fsm.handle(
                E::ChildAcknowledged(StageAck {
                    stage_id: id,
                    milestone: StageMilestone::Started,
                    snapshot: crate::stages::common::stage_lifecycle::StageSnapshot {
                        causal_context: frontier,
                        ..Default::default()
                    },
                }),
                &mut ctx,
            )
            .await
            .unwrap();
            let parent = publication::append(
                &journal,
                SystemEventFactory::new(ctx.system_id).pipeline_ready_for_run(Some(1)),
                Default::default(),
            )
            .await
            .unwrap();
            crate::testing::assert_happens_before(&record, &parent).unwrap();
        })
        .await;
}
