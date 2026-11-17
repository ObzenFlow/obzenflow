// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_infra::verify::{verify_run_dirs, RefusalReason, VerifyOptions, VerifyOutcome};

#[tokio::test]
async fn verification_refuses_an_unavailable_archive() {
    let temp = tempfile::tempdir().expect("tempdir");
    let missing = temp.path().join("no-such-run");
    let outcome = verify_run_dirs(&missing, &missing, &VerifyOptions::default())
        .expect("refusals are outcomes, not errors");
    assert_eq!(outcome.exit_code(), 3);
    assert!(matches!(
        outcome,
        VerifyOutcome::Refused(RefusalReason::ArchiveUnavailable { .. })
    ));
}
