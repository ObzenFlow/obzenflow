// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Guards the handler-style, value-owned output contract of public sources.

use std::fs;
use std::path::PathBuf;

#[test]
fn public_source_construction_keeps_output_on_the_decoder_value() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let csv = fs::read_to_string(root.join("crates/obzenflow_infra/src/systems/file/csv.rs"))
        .expect("read CSV source");
    let csv_example = fs::read_to_string(root.join("examples/csv_demo_support_sla/flow.rs"))
        .expect("read CSV example");
    let yaml_example =
        fs::read_to_string(root.join("examples/http_ingestion_piggy_bank_demo/flow.rs"))
            .expect("read YAML example");
    let yaml = fs::read_to_string(root.join("crates/obzenflow_infra/src/systems/file/yaml/mod.rs"))
        .expect("read YAML source");

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

    assert!(csv_example.contains("CsvSource::builder(CustomerCsv)"));
    assert!(csv_example.contains("CsvSource::builder(TicketCsv)"));
    assert!(yaml.contains("pub trait YamlDecoder"));
    assert!(yaml.contains("type Output: OneFactOutcome"));
    assert!(yaml.contains("pub fn builder(decoder: D)"));

    assert!(yaml_example.contains("YamlSource::builder(AccountYaml)"));
}
