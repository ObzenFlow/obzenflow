//! FLOWIP-114b acceptance criterion #6: contract-drift regression.
//!
//! After the canonical `obzenflow_topology::Topology` became the
//! `/api/topology` wire contract, the infra layer must not reintroduce
//! any private mirror of that response. This test fails the build if any
//! `struct FlowTopologyResponse`, `struct StageApiInfo`, or
//! `struct EdgeApiInfo` definition shows up in this crate's source tree
//! or test tree.

use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN_STRUCT_NAMES: &[&str] = &[
    "FlowTopologyResponse",
    "StageApiInfo",
    "EdgeApiInfo",
    "JoinMetadataApiInfo",
    "MiddlewareApiInfo",
    "ContractApiInfo",
    "StageTypingApiInfo",
    "TypeHintApiInfo",
    "EdgeTypingApiInfo",
    "StageSubgraphApiInfo",
    "SubgraphApiInfo",
];

#[test]
fn no_topology_response_dto_is_redefined() {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut violations: Vec<String> = Vec::new();
    for sub in ["src", "tests"] {
        let dir = crate_root.join(sub);
        walk_rs_files(&dir, &mut |path, contents| {
            if path.file_name().and_then(|n| n.to_str()) == Some("no_topology_dto_drift.rs") {
                return;
            }
            for name in FORBIDDEN_STRUCT_NAMES {
                for pattern in [
                    format!("struct {name} "),
                    format!("struct {name}{{"),
                    format!("struct {name}("),
                    format!("struct {name};"),
                    format!("struct {name}\n"),
                ] {
                    if contents.contains(&pattern) {
                        violations.push(format!(
                            "{} redefines `{name}` — the canonical \
                             obzenflow_topology::Topology is the wire contract \
                             and infra-private mirrors must not be reintroduced \
                             (FLOWIP-114b acceptance #6)",
                            path.display()
                        ));
                    }
                }
            }
        });
    }
    assert!(
        violations.is_empty(),
        "topology DTO drift detected:\n{}",
        violations.join("\n")
    );
}

fn walk_rs_files(dir: &Path, visit: &mut dyn FnMut(&Path, &str)) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_rs_files(&path, visit);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            if let Ok(contents) = fs::read_to_string(&path) {
                visit(&path, &contents);
            }
        }
    }
}
