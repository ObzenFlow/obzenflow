// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Guards the handler-style, value-owned output contract of public sources.

use std::fs;
use std::path::PathBuf;

#[test]
fn public_source_construction_keeps_output_on_the_decoder_value() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let csv = fs::read_to_string(root.join("crates/obzenflow_adapters/src/sources/csv.rs"))
        .expect("read CSV source");
    let pull = fs::read_to_string(root.join("crates/obzenflow_adapters/src/sources/http_pull.rs"))
        .expect("read HTTP pull source");
    let ingress = fs::read_to_string(
        root.join("crates/obzenflow_infra/src/web/endpoints/event_ingestion/mod.rs"),
    )
    .expect("read hosted ingress source");
    let csv_example = fs::read_to_string(root.join("examples/csv_demo_support_sla/flow.rs"))
        .expect("read CSV example");
    let ingress_example =
        fs::read_to_string(root.join("examples/http_ingestion_piggy_bank_demo/runner.rs"))
            .expect("read hosted ingress example");

    assert!(csv.contains("pub trait CsvDecoder"));
    assert!(csv.contains("type Output:"));
    assert!(csv.contains("pub fn builder(decoder: D)"));
    for retired in [
        "typed_builder",
        "typed_from_file",
        "typed_tsv_from_file",
        "pub fn builder()",
    ] {
        assert!(
            !csv.contains(retired),
            "retired CSV source API returned: {retired}"
        );
    }

    assert!(pull.contains("pub trait PullDecoder"));
    assert!(pull.contains("pub trait CursorlessPullDecoder"));
    assert!(!pull.contains("type Item: TypedPayload"));
    assert!(!pull.contains("D::Item"));
    assert!(!pull.contains("fn event_type(&self)"));

    assert!(ingress.contains("pub fn ingress_source<D>(decoder: D"));
    assert!(ingress.contains("pub fn http_ingress<D>(decoder: D"));
    assert!(!ingress.contains("pub fn http_ingress<T>"));

    assert!(csv_example.contains("CsvSource::builder(CustomerCsv)"));
    assert!(csv_example.contains("CsvSource::builder(TicketCsv)"));
    assert!(ingress_example.contains("http_ingress(AccountIngress,"));
    assert!(ingress_example.contains("http_ingress(LedgerIngress,"));
}
