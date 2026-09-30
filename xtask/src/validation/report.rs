// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::plan::TestId;
use crate::{error, Result};
use serde::Serialize;
use std::collections::BTreeSet;

#[derive(Debug, Serialize)]
pub(super) struct TestResults {
    pub(super) observed: BTreeSet<TestId>,
    pub(super) failed: BTreeSet<TestId>,
    pub(super) attempts: usize,
}

/// The report is independent evidence. In particular, a zero child exit does
/// not erase a failure in an earlier retry or stress iteration (145i).
pub(super) fn inspect(xml: &str) -> Result<TestResults> {
    let doc = roxmltree::Document::parse(xml)?;
    let root = doc.root_element();
    if root.tag_name().name() != "testsuites" {
        return Err(error("JUnit root must be testsuites"));
    }
    let declared: usize = root
        .attribute("tests")
        .ok_or_else(|| error("JUnit has no total test count"))?
        .parse()?;
    let mut results = TestResults {
        observed: BTreeSet::new(),
        failed: BTreeSet::new(),
        attempts: 0,
    };
    let mut seen = BTreeSet::new();
    for suite in root
        .children()
        .filter(|node| node.has_tag_name("testsuite"))
    {
        let suite_name = suite
            .attribute("name")
            .ok_or_else(|| error("unnamed JUnit suite"))?;
        let declared_suite: usize = suite
            .attribute("tests")
            .ok_or_else(|| error("JUnit suite has no test count"))?
            .parse()?;
        let mut suite_summary_failed = false;
        for attribute in ["failures", "errors"] {
            let count: usize = suite
                .attribute(attribute)
                .ok_or_else(|| error(format!("JUnit suite has no {attribute} count")))?
                .parse()?;
            suite_summary_failed |= count != 0;
        }
        let mut suite_count = 0;
        let mut suite_failed = false;
        for case in suite
            .children()
            .filter(|node| node.has_tag_name("testcase"))
        {
            let name = case
                .attribute("name")
                .ok_or_else(|| error("unnamed JUnit test"))?;
            let binary = case
                .attribute("classname")
                .ok_or_else(|| error("JUnit test has no binary identity"))?;
            let id = TestId {
                binary: binary.into(),
                test: name.into(),
            };
            if !seen.insert((suite_name, binary, name)) {
                return Err(error("duplicate JUnit test within one iteration"));
            }
            if case.children().any(|node| node.has_tag_name("skipped")) {
                return Err(error(format!(
                    "unexpected skipped result: {binary}::{name}"
                )));
            }
            let failed = case.descendants().any(|node| {
                matches!(
                    node.tag_name().name(),
                    "failure"
                        | "error"
                        | "flakyFailure"
                        | "flakyError"
                        | "rerunFailure"
                        | "rerunError"
                )
            });
            if failed {
                results.failed.insert(id.clone());
                suite_failed = true;
            }
            results.observed.insert(id);
            results.attempts += 1;
            suite_count += 1;
        }
        if suite_count != declared_suite {
            return Err(error(format!("incomplete JUnit suite: {suite_name}")));
        }
        if suite_summary_failed && !suite_failed {
            return Err(error(format!(
                "JUnit suite failure summary has no corresponding test failure: {suite_name}"
            )));
        }
    }
    if results.attempts == 0 || results.attempts != declared {
        return Err(error("empty or incomplete JUnit report"));
    }
    // Summary failure counts must not contradict the per-test evidence.
    for attr in ["failures", "errors"] {
        let count: usize = root
            .attribute(attr)
            .ok_or_else(|| error(format!("JUnit has no {attr} count")))?
            .parse()?;
        if count != 0 && results.failed.is_empty() {
            return Err(error(
                "JUnit failure summary has no corresponding test failure",
            ));
        }
    }
    Ok(results)
}

pub(super) fn accept(
    results: &TestResults,
    expected: &BTreeSet<TestId>,
    exit_success: bool,
) -> Result<()> {
    if !results.failed.is_empty() {
        return Err(super::failed(format!(
            "{} test identities have failed attempts; later success cannot erase them: {:?}",
            results.failed.len(),
            results.failed
        )));
    }
    if !exit_success {
        return Err(super::failed(
            "test process failed, regardless of its report",
        ));
    }
    if &results.observed != expected {
        return Err(error(format!(
            "test coverage mismatch: missing={:?}, unexpected={:?}",
            expected.difference(&results.observed).collect::<Vec<_>>(),
            results.observed.difference(expected).collect::<Vec<_>>()
        )));
    }
    if results.attempts != expected.len() {
        return Err(error("acceptance requires exactly one attempt per selected test; stress/retry reports are supplementary"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const PASS: &str = r#"<testsuites tests="1" failures="0" errors="0"><testsuite name="bin" tests="1" failures="0" errors="0"><testcase classname="bin" name="test"/></testsuite></testsuites>"#;
    fn expected() -> BTreeSet<TestId> {
        BTreeSet::from([TestId {
            binary: "bin".into(),
            test: "test".into(),
        }])
    }

    #[test]
    fn early_stress_failure_cannot_be_erased_by_last_iteration_or_zero_exit() {
        let xml = r#"<testsuites tests="2" failures="1" errors="0"><testsuite name="bin@stress-0" tests="1" failures="1" errors="0"><testcase classname="bin" name="test"><failure>lost output</failure></testcase></testsuite><testsuite name="bin@stress-1" tests="1" failures="0" errors="0"><testcase classname="bin" name="test"/></testsuite></testsuites>"#;
        let report = inspect(xml).unwrap();
        assert_eq!(report.attempts, 2);
        assert!(accept(&report, &expected(), true).is_err());
    }
    #[test]
    fn retries_missing_results_and_child_failure_are_not_acceptance() {
        let flaky = PASS.replace(
            "name=\"test\"/>",
            "name=\"test\"><flakyFailure>first attempt</flakyFailure></testcase>",
        );
        assert!(accept(&inspect(&flaky).unwrap(), &expected(), true).is_err());
        assert!(accept(&inspect(PASS).unwrap(), &BTreeSet::new(), true).is_err());
        assert!(accept(&inspect(PASS).unwrap(), &expected(), false).is_err());
        assert!(accept(&inspect(PASS).unwrap(), &expected(), true).is_ok());
    }
    #[test]
    fn truncated_empty_skipped_and_duplicate_reports_fail_closed() {
        for xml in [
            "",
            "<testsuites>",
            "<testsuites tests=\"0\" failures=\"0\" errors=\"0\"/>",
        ] {
            assert!(inspect(xml).is_err());
        }
        assert!(inspect(&PASS.replace("tests=\"1\"", "tests=\"2\"")).is_err());
        assert!(
            inspect(&PASS.replace("name=\"test\"/>", "name=\"test\"><skipped/></testcase>"))
                .is_err()
        );
    }

    #[test]
    fn suite_failure_summary_cannot_be_erased_by_root_summary_or_zero_exit() {
        for attribute in ["failures", "errors"] {
            let xml = PASS.replace(
                "<testsuite name=\"bin\" tests=\"1\" failures=\"0\" errors=\"0\">",
                &format!(
                    "<testsuite name=\"bin\" tests=\"1\" failures=\"{}\" errors=\"{}\">",
                    usize::from(attribute == "failures"),
                    usize::from(attribute == "errors"),
                ),
            );
            assert!(
                inspect(&xml).is_err(),
                "contradictory suite {attribute} cannot become acceptance"
            );
        }
    }
}
