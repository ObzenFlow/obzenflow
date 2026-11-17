// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::exported_jsonl;

#[test]
#[should_panic(expected = "is neither a ChainEvent nor a SystemEvent")]
fn exported_record_decoder_fails_loud_on_an_unknown_row() {
    exported_jsonl::chain_events(r#"{"event":{"unknown":"shape"}}"#);
}
