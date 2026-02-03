// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::{
    event::{
        chain_event::{ChainEvent, ChainEventFactory},
        status::processing_status::{ErrorKind, ProcessingStatus},
    },
    id::StageId,
    WriterId,
};
use serde_json::json;

#[test]
fn processing_status_error_round_trips_with_kind() {
    let status = ProcessingStatus::error_with_kind("boom", Some(ErrorKind::Timeout));

    let encoded = serde_json::to_string(&status).expect("encode ProcessingStatus");
    let decoded: ProcessingStatus =
        serde_json::from_str(&encoded).expect("decode ProcessingStatus");

    match decoded {
        ProcessingStatus::Error { message, kind } => {
            assert_eq!(message, "boom");
            assert!(matches!(kind, Some(ErrorKind::Timeout)));
        }
        other => panic!("expected Error variant, got {other:?}"),
    }
}

#[test]
fn processing_status_error_helpers_set_kind() {
    let generic = ProcessingStatus::error("oops");
    match generic {
        ProcessingStatus::Error { message, kind } => {
            assert_eq!(message, "oops");
            assert!(kind.is_none());
        }
        other => panic!("expected Error variant, got {other:?}"),
    }

    let typed = ProcessingStatus::error_with_kind("validation failed", Some(ErrorKind::Validation));
    match typed {
        ProcessingStatus::Error { message, kind } => {
            assert_eq!(message, "validation failed");
            assert!(matches!(kind, Some(ErrorKind::Validation)));
        }
        other => panic!("expected Error variant, got {other:?}"),
    }
}

#[test]
fn processing_status_kind_accessor_matches_variant() {
    let timeout = ProcessingStatus::error_with_kind("t", Some(ErrorKind::Timeout));
    assert!(matches!(timeout.kind(), Some(ErrorKind::Timeout)));

    let domain = ProcessingStatus::error_with_kind("d", Some(ErrorKind::Domain));
    assert!(matches!(domain.kind(), Some(ErrorKind::Domain)));

    let success = ProcessingStatus::success();
    assert!(success.kind().is_none());
}

#[test]
fn chain_event_mark_as_error_sets_status_and_hop_budget() {
    let writer_id = WriterId::from(StageId::new());
    let base: ChainEvent =
        ChainEventFactory::data_event(writer_id, "test.event", json!({ "key": "value" }));

    let err = base
        .clone()
        .mark_as_error("deser failed", ErrorKind::Deserialization);
    match err.processing_info.status {
        ProcessingStatus::Error {
            ref message,
            ref kind,
        } => {
            assert_eq!(message, "deser failed");
            assert!(matches!(kind, Some(ErrorKind::Deserialization)));
        }
        ref other => panic!("expected Error status, got {other:?}"),
    }
    assert_eq!(err.processing_info.error_hops_remaining, Some(1));
}

#[test]
fn chain_event_validation_and_infra_helpers_set_expected_kinds() {
    let writer_id = WriterId::from(StageId::new());
    let base: ChainEvent = ChainEventFactory::data_event(writer_id, "flight", json!({ "id": 1 }));

    let validation = base.clone().mark_as_validation_error("bad input");
    match validation.processing_info.status {
        ProcessingStatus::Error {
            ref message,
            ref kind,
        } => {
            assert_eq!(message, "bad input");
            assert!(matches!(kind, Some(ErrorKind::Validation)));
        }
        ref other => panic!("expected Error status, got {other:?}"),
    }
    assert_eq!(validation.processing_info.error_hops_remaining, Some(1));

    let infra = base.mark_as_infra_error("remote timeout");
    match infra.processing_info.status {
        ProcessingStatus::Error {
            ref message,
            ref kind,
        } => {
            assert_eq!(message, "remote timeout");
            assert!(matches!(kind, Some(ErrorKind::Remote)));
        }
        ref other => panic!("expected Error status, got {other:?}"),
    }
    assert_eq!(infra.processing_info.error_hops_remaining, Some(1));
}
