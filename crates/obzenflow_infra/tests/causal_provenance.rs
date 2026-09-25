// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{CausalCommit, CausalFrontier, ChainEventFactory, SystemEvent};
use obzenflow_core::journal::causal::{CausalProof, CausalProofCache};
use obzenflow_core::journal::{AppendOptions, Journal};
use obzenflow_core::JournalOwner;
use obzenflow_core::{ChainEvent, FlowId, StageId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use std::sync::Arc;

#[tokio::test]
async fn journal_scoped_fanout_reconvergence_cross_family_and_private_groups() {
    for disk in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let run = FlowId::new();
        let author = StageId::new();
        let owner = JournalOwner::stage(author);
        let chains: Vec<Arc<dyn Journal<ChainEvent>>> = (0..4)
            .map(|index| {
                if disk {
                    Arc::new(
                        DiskJournal::with_owner_in_run(
                            directory.path().join(format!("{index}.log")),
                            owner.clone(),
                            run,
                        )
                        .unwrap(),
                    ) as Arc<dyn Journal<ChainEvent>>
                } else {
                    Arc::new(MemoryJournal::with_owner_in_run(owner.clone(), run))
                        as Arc<dyn Journal<ChainEvent>>
                }
            })
            .collect();
        let system: Arc<dyn Journal<SystemEvent>> = if disk {
            Arc::new(
                DiskJournal::with_owner_in_run(
                    directory.path().join("system.log"),
                    owner.clone(),
                    run,
                )
                .unwrap(),
            )
        } else {
            Arc::new(MemoryJournal::with_owner_in_run(owner, run))
        };
        let event =
            || ChainEventFactory::data_event(author.into(), "causal.fact", serde_json::json!({}));
        let source = chains[0].append(event(), Default::default()).await.unwrap();
        let left = chains[1]
            .append(
                source.authored(),
                AppendOptions::from_record(Some(&source)).unwrap(),
            )
            .await
            .unwrap();
        let right = chains[2]
            .append(
                source.authored(),
                AppendOptions::from_record(Some(&source)).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(left.id(), right.id(), "forwarded identity is immutable");
        assert_ne!(left.causal_coordinate(), right.causal_coordinate());
        assert!(CausalOrderingService::are_concurrent(
            &left.envelope.provenance.journal.vector_clock,
            &right.envelope.provenance.journal.vector_clock
        ));
        let mut frontier = CausalFrontier::from_record(&left).unwrap();
        frontier
            .merge(&CausalFrontier::from_record(&right).unwrap())
            .unwrap();
        let joined = chains[3]
            .append(event(), AppendOptions::new(frontier))
            .await
            .unwrap();
        let lifecycle = system
            .append(
                SystemEvent::stage_completed(author),
                AppendOptions::from_record(Some(&joined)).unwrap(),
            )
            .await
            .unwrap();
        let unrelated = system
            .append(
                SystemEvent::stage_running(StageId::new()),
                Default::default(),
            )
            .await
            .unwrap();
        assert!(CausalOrderingService::are_concurrent(
            &lifecycle.envelope.provenance.journal.vector_clock,
            &unrelated.envelope.provenance.journal.vector_clock
        ));
        let authorised = chains[3]
            .append(
                event(),
                AppendOptions::from_record(Some(&lifecycle)).unwrap(),
            )
            .await
            .unwrap();
        let other_author = StageId::new();
        let members = chains[3]
            .append_group(
                "causal-group",
                vec![
                    event(),
                    ChainEventFactory::data_event(
                        other_author.into(),
                        "causal.other",
                        serde_json::json!({}),
                    ),
                    event(),
                ],
                Default::default(),
            )
            .await
            .unwrap();
        for pair in members.windows(2) {
            assert!(CausalOrderingService::happened_before(
                &pair[0].envelope.provenance.journal.vector_clock,
                &pair[1].envelope.provenance.journal.vector_clock
            ));
        }
        let mut cache = CausalProofCache::new(100, 1000);
        for record in [&source, &left, &right, &joined] {
            let commitment = CausalCommit::from_record(record).unwrap();
            assert!(matches!(
                cache.verify(&commitment, &record.envelope.provenance.journal.causal),
                CausalProof::Valid { .. }
            ));
            cache.admit(commitment).unwrap();
        }
        for record in [&lifecycle, &unrelated] {
            let commitment = CausalCommit::from_record(record).unwrap();
            assert!(matches!(
                cache.verify(&commitment, &record.envelope.provenance.journal.causal),
                CausalProof::Valid { .. }
            ));
            cache.admit(commitment).unwrap();
        }
        for record in std::iter::once(&authorised).chain(&members) {
            let commitment = CausalCommit::from_record(record).unwrap();
            assert!(matches!(
                cache.verify(&commitment, &record.envelope.provenance.journal.causal),
                CausalProof::Valid { .. }
            ));
            cache.admit(commitment).unwrap();
        }
    }
}

#[tokio::test]
async fn empty_and_populated_reopen_preserve_identity_with_surviving_readers() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stable.log");
    let author = StageId::new();
    let owner = JournalOwner::stage(author);
    let journal = DiskJournal::<ChainEvent>::with_owner(path.clone(), owner.clone()).unwrap();
    let id = *journal.id();
    let mut reader = journal.reader().await.unwrap();
    assert!(DiskJournal::<ChainEvent>::with_owner(path.clone(), owner.clone()).is_err());
    let clone = journal.clone();
    drop(journal);
    assert!(DiskJournal::<ChainEvent>::with_owner(path.clone(), owner.clone()).is_err());
    drop(clone);
    let reopened = DiskJournal::<ChainEvent>::with_owner(path.clone(), owner.clone()).unwrap();
    assert_eq!(reopened.id(), &id);
    let first = reopened
        .append(
            ChainEventFactory::data_event(author.into(), "root", serde_json::json!({})),
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().id(), first.id());
    drop(reopened);
    let reopened = DiskJournal::<ChainEvent>::with_owner(path, owner).unwrap();
    let second = reopened
        .append(first.authored(), Default::default())
        .await
        .unwrap();
    assert_eq!(second.local_sequence(), 2);
    assert_eq!(
        second.envelope.provenance.journal.causal.previous,
        Some(CausalCommit::from_record(&first).unwrap().reference)
    );
    assert_eq!(reader.next().await.unwrap().unwrap().local_sequence(), 2);
}
