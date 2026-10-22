// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The lineage check for simulated systems' call logs: each log is judged
//! against the lineage its runs' manifests record.

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};

use obzenflow_core::journal::archive::manifest::RunManifest;
use serde::de::IgnoredAny;

use super::error::{RefusalReason, VerifyError};
use super::source::{read_manifest, SourceOpenError};
use crate::simulated::{read_lines, CallLogError, CallLogLine};

/// The lineage check's result: a refusal of its input, or one judgment per
/// call log.
#[derive(Debug, Clone)]
pub enum CallLogCheck {
    /// The run directories name no single lineage.
    Refused(RefusalReason),
    /// Each call log judged against the lineage started by `first_run`.
    Judged {
        first_run: String,
        logs: Vec<CallLogJudgment>,
    },
}

/// One call log's judgment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallLogJudgment {
    pub path: PathBuf,
    /// Every call the log holds.
    pub calls: u64,
    /// The calls answered from memory.
    pub replayed_by_resumes: u64,
    pub verdict: CallLogVerdict,
}

/// What the check shows about one call log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallLogVerdict {
    /// The log breaks no rule and holds a call from the lineage's first run.
    Passed,
    /// Holds no call from the lineage's first run, so the log cannot show it is complete.
    Uncertified,
    /// The log breaks a rule.
    Violated(CallLogViolation),
}

/// A rule a call log breaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallLogViolation {
    /// Calls from a run that is not part of the lineage.
    OutsideLineage { flow_id: String, calls: u64 },
    /// A key decided by two calls of the lineage, from the runs named.
    DecidedTwice {
        key: String,
        first: String,
        second: String,
    },
    /// More calls answered from memory than the lineage has resumes.
    TooManyRepeats { replayed: u64 },
}

impl CallLogCheck {
    /// The exit code `obzenflow verify calls` reports: `0` when every log
    /// passes, `1` when a log breaks a rule, `2` when a log is uncertified and
    /// none breaks a rule, and `3` when the input is refused.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Refused(_) => 3,
            Self::Judged { logs, .. } => {
                let verdicts = || logs.iter().map(|log| &log.verdict);
                if verdicts().any(|verdict| matches!(verdict, CallLogVerdict::Violated(_))) {
                    1
                } else if verdicts().any(|verdict| *verdict == CallLogVerdict::Uncertified) {
                    2
                } else {
                    0
                }
            }
        }
    }
}

/// Judges each call log against the lineage that `run_dirs` record, given in
/// any order.
///
/// The lineage's first run is the manifests' shared `recorded_flow_id`, and
/// its resumes are counted by their `resume` blocks. A call log that does not
/// exist holds no calls.
///
/// # Errors
///
/// Fails when a manifest or call log cannot be read, or a log line does not
/// parse.
pub fn verify_call_logs(
    run_dirs: &[PathBuf],
    call_logs: &[PathBuf],
) -> Result<CallLogCheck, VerifyError> {
    let mut manifests = Vec::with_capacity(run_dirs.len());
    for run_dir in run_dirs {
        match read_manifest(run_dir) {
            Ok(manifest) => manifests.push(manifest),
            Err(SourceOpenError::Refused(reason)) => return Ok(CallLogCheck::Refused(*reason)),
            Err(SourceOpenError::Failed(error)) => return Err(error),
        }
    }
    let lineage = match Lineage::of(&manifests) {
        Ok(lineage) => lineage,
        Err(reason) => return Ok(CallLogCheck::Refused(reason)),
    };
    let logs = call_logs
        .iter()
        .map(|path| judge(path, &lineage))
        .collect::<Result<_, _>>()?;
    Ok(CallLogCheck::Judged {
        first_run: lineage.first_run,
        logs,
    })
}

/// The lines `obzenflow verify calls` prints, one for each call log.
pub fn render_call_log_check(check: &CallLogCheck) -> String {
    match check {
        CallLogCheck::Refused(reason) => format!("error: {reason}"),
        CallLogCheck::Judged { first_run, logs } => logs
            .iter()
            .map(|log| render_judgment(log, first_run))
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn render_judgment(log: &CallLogJudgment, first_run: &str) -> String {
    let path = log.path.display();
    match &log.verdict {
        CallLogVerdict::Passed => format!(
            "ok: {path} holds {} calls from this lineage, {} repeated by a resume.",
            log.calls, log.replayed_by_resumes
        ),
        CallLogVerdict::Uncertified => {
            let holds = if log.path.exists() {
                format!("{path} holds no calls")
            } else {
                format!("{path} does not exist, so it holds no calls")
            };
            format!(
                "uncertified: {holds} from {first_run}, which started this lineage.\n  \
                 That run may never have called this system, or the ledger was cleared after \
                 it. A resume never clears the ledger."
            )
        }
        CallLogVerdict::Violated(CallLogViolation::OutsideLineage { flow_id, calls }) => format!(
            "error: {path} holds {calls} calls from {flow_id}, which is not part of the lineage \
             started by {first_run}. List every run of the lineage. A fresh run starts with an \
             empty ledger."
        ),
        CallLogVerdict::Violated(CallLogViolation::DecidedTwice { key, first, second }) => {
            format!("error: {key} was decided twice in this lineage ({first}, {second}).")
        }
        CallLogVerdict::Violated(CallLogViolation::TooManyRepeats { replayed }) => format!(
            "error: {path} holds {replayed} calls repeated by resumes; each resume may repeat \
             only the call its kill cut off."
        ),
    }
}

struct Lineage {
    first_run: String,
    runs: BTreeSet<String>,
    resumes: u64,
}

impl Lineage {
    fn of(manifests: &[RunManifest]) -> Result<Self, RefusalReason> {
        let first_runs: Vec<String> = manifests
            .iter()
            .map(|manifest| manifest.lineage_namespace().to_owned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let first_run = match first_runs.as_slice() {
            [] => return Err(RefusalReason::NoRunDirectories),
            [first_run] => first_run.clone(),
            _ => return Err(RefusalReason::SeveralLineages { first_runs }),
        };
        let mut runs = BTreeSet::from([first_run.clone()]);
        let mut resumes = 0;
        for manifest in manifests {
            if runs.insert(manifest.flow_id.clone()) && manifest.resume.is_some() {
                resumes += 1;
            }
        }
        Ok(Self {
            first_run,
            runs,
            resumes,
        })
    }
}

fn judge(path: &Path, lineage: &Lineage) -> Result<CallLogJudgment, VerifyError> {
    let mut tally = Tally::default();
    match File::open(path) {
        Ok(file) => read_lines(path, file, |line: CallLogLine<IgnoredAny, IgnoredAny>| {
            tally.add(line, lineage);
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(CallLogError::Io {
                path: path.to_path_buf(),
                source,
            }
            .into())
        }
    }
    Ok(CallLogJudgment {
        path: path.to_path_buf(),
        calls: tally.calls,
        replayed_by_resumes: tally.replayed,
        verdict: tally.verdict(lineage),
    })
}

#[derive(Default)]
struct Tally {
    calls: u64,
    replayed: u64,
    from_first_run: bool,
    runs: Vec<(String, u64)>,
    first_outside: Option<usize>,
    decided_by: HashMap<String, usize>,
    decided_twice: Option<(String, usize, usize)>,
}

impl Tally {
    fn add(&mut self, line: CallLogLine<IgnoredAny, IgnoredAny>, lineage: &Lineage) {
        let (key, flow_id, decided) = match line {
            CallLogLine::Decided { key, flow_id, .. } => (key, flow_id, true),
            CallLogLine::Failed { key, flow_id, .. } => (key, flow_id, false),
            CallLogLine::Replayed { key, flow_id } => {
                self.replayed += 1;
                (key, flow_id, false)
            }
        };
        self.calls += 1;
        self.from_first_run |= flow_id == lineage.first_run;
        let run = self.run(flow_id, lineage);
        if !decided {
            return;
        }
        match self.decided_by.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(run);
            }
            Entry::Occupied(entry) => {
                if self.decided_twice.is_none() {
                    self.decided_twice = Some((entry.key().clone(), *entry.get(), run));
                }
            }
        }
    }

    fn run(&mut self, flow_id: String, lineage: &Lineage) -> usize {
        let index = match self.runs.iter().position(|(seen, _)| *seen == flow_id) {
            Some(index) => index,
            None => {
                if !lineage.runs.contains(&flow_id) && self.first_outside.is_none() {
                    self.first_outside = Some(self.runs.len());
                }
                self.runs.push((flow_id, 0));
                self.runs.len() - 1
            }
        };
        self.runs[index].1 += 1;
        index
    }

    fn verdict(&self, lineage: &Lineage) -> CallLogVerdict {
        if let Some(outside) = self.first_outside {
            let (flow_id, calls) = self.runs[outside].clone();
            return CallLogVerdict::Violated(CallLogViolation::OutsideLineage { flow_id, calls });
        }
        if let Some((key, first, second)) = &self.decided_twice {
            return CallLogVerdict::Violated(CallLogViolation::DecidedTwice {
                key: key.clone(),
                first: self.runs[*first].0.clone(),
                second: self.runs[*second].0.clone(),
            });
        }
        if self.replayed > lineage.resumes {
            return CallLogVerdict::Violated(CallLogViolation::TooManyRepeats {
                replayed: self.replayed,
            });
        }
        if self.from_first_run {
            CallLogVerdict::Passed
        } else {
            CallLogVerdict::Uncertified
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use obzenflow_core::journal::archive::manifest::{
        RunManifestResumeConfig, JOURNAL_SCHEMA_VERSION,
    };
    use std::collections::BTreeMap;

    const FIRST: &str = "flow_01M50KILLED";
    const RESUMED: &str = "flow_01M50RESUMED";
    const OTHER: &str = "flow_01M50OTHERRUN";

    fn manifest(flow_id: &str, lineage: &str, resumed: bool) -> RunManifest {
        RunManifest {
            journal_schema_version: JOURNAL_SCHEMA_VERSION.to_string(),
            obzenflow_version: "0.2.6".to_string(),
            flow_id: flow_id.to_string(),
            recorded_flow_id: Some(lineage.to_string()),
            pipeline_writer_id: obzenflow_core::SystemId::new().into(),
            flow_name: "aml_overnight_monitoring".to_string(),
            created_at: Utc::now(),
            replay: None,
            resume: resumed.then(|| RunManifestResumeConfig {
                resumed_from: PathBuf::from("killed"),
                resume_generation: 1,
                merged_from: Vec::new(),
            }),
            stages: HashMap::new(),
            system_journal_file: "system.log".to_string(),
            metrics_journals: None,
            effective_config: None,
            capabilities: BTreeMap::new(),
            bounded_direct_fact_admission: Vec::new(),
        }
    }

    fn killed_and_resumed() -> Vec<RunManifest> {
        vec![
            manifest(FIRST, FIRST, false),
            manifest(RESUMED, FIRST, true),
        ]
    }

    fn decided(key: &str, flow_id: &str) -> String {
        format!(
            r#"{{"result":"decided","key":"{key}","flow_id":"{flow_id}","response":{{"case_number":1}}}}"#
        )
    }

    fn failed(key: &str, flow_id: &str) -> String {
        format!(
            r#"{{"result":"failed","key":"{key}","flow_id":"{flow_id}","error":{{"reason":"timed_out"}}}}"#
        )
    }

    fn replayed(key: &str, flow_id: &str) -> String {
        format!(r#"{{"result":"replayed","key":"{key}","flow_id":"{flow_id}"}}"#)
    }

    fn judged(manifests: &[RunManifest], lines: &[String]) -> CallLogJudgment {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("calls.jsonl");
        let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
        std::fs::write(&path, text).expect("write the log");
        let lineage = Lineage::of(manifests).expect("one lineage");
        judge(&path, &lineage).expect("the log is judged")
    }

    fn verdict(manifests: &[RunManifest], lines: &[String]) -> CallLogVerdict {
        judged(manifests, lines).verdict
    }

    #[test]
    fn a_control_passes_with_no_repeats() {
        let control = [manifest(FIRST, FIRST, false)];
        let judgment = judged(
            &control,
            &[decided("aml-score:1", FIRST), decided("aml-score:2", FIRST)],
        );
        assert_eq!(judgment.verdict, CallLogVerdict::Passed);
        assert_eq!((judgment.calls, judgment.replayed_by_resumes), (2, 0));
    }

    #[test]
    fn a_killed_run_and_its_resume_pass_in_either_order() {
        let lines = [
            decided("aml-score:1", FIRST),
            decided("aml-score:2", FIRST),
            replayed("aml-score:2", RESUMED),
            decided("aml-score:3", RESUMED),
        ];
        let mut manifests = killed_and_resumed();
        assert_eq!(verdict(&manifests, &lines), CallLogVerdict::Passed);
        manifests.reverse();
        let judgment = judged(&manifests, &lines);
        assert_eq!(judgment.verdict, CallLogVerdict::Passed);
        assert_eq!((judgment.calls, judgment.replayed_by_resumes), (4, 1));
    }

    #[test]
    fn a_call_from_a_run_outside_the_lineage_breaks_a_rule() {
        let lines = [
            decided("aml-score:1", OTHER),
            decided("aml-score:2", OTHER),
            decided("aml-score:1", FIRST),
        ];
        assert_eq!(
            verdict(&killed_and_resumed(), &lines),
            CallLogVerdict::Violated(CallLogViolation::OutsideLineage {
                flow_id: OTHER.to_string(),
                calls: 2,
            })
        );
    }

    #[test]
    fn a_key_decided_twice_breaks_a_rule() {
        let lines = [
            decided("aml-score:1", FIRST),
            decided("aml-score:1", RESUMED),
        ];
        assert_eq!(
            verdict(&killed_and_resumed(), &lines),
            CallLogVerdict::Violated(CallLogViolation::DecidedTwice {
                key: "aml-score:1".to_string(),
                first: FIRST.to_string(),
                second: RESUMED.to_string(),
            })
        );
    }

    #[test]
    fn more_repeats_than_resumes_break_a_rule() {
        let lines = [
            decided("aml-score:1", FIRST),
            decided("aml-score:2", FIRST),
            replayed("aml-score:1", RESUMED),
            replayed("aml-score:2", RESUMED),
        ];
        assert_eq!(
            verdict(&killed_and_resumed(), &lines),
            CallLogVerdict::Violated(CallLogViolation::TooManyRepeats { replayed: 2 })
        );
    }

    #[test]
    fn a_failed_attempt_neither_decides_a_key_nor_counts_as_a_repeat() {
        let lines = [
            failed("payment-authorize:web-1", FIRST),
            failed("payment-authorize:web-1", RESUMED),
            decided("payment-authorize:web-1", RESUMED),
            decided("payment-authorize:web-2", FIRST),
        ];
        let judgment = judged(&killed_and_resumed(), &lines);
        assert_eq!(judgment.verdict, CallLogVerdict::Passed);
        assert_eq!(judgment.replayed_by_resumes, 0);
    }

    #[test]
    fn a_log_with_no_call_from_the_first_run_is_uncertified() {
        let never_called = [decided("payment-authorize:W-01", RESUMED)];
        assert_eq!(
            verdict(&killed_and_resumed(), &never_called),
            CallLogVerdict::Uncertified
        );
        let emptied_before_the_resume = [
            decided("aml-score:2", RESUMED),
            decided("aml-score:3", RESUMED),
        ];
        assert_eq!(
            verdict(&killed_and_resumed(), &emptied_before_the_resume),
            CallLogVerdict::Uncertified,
            "an emptied log leaves the same evidence"
        );
    }

    #[test]
    fn a_rule_break_outranks_a_missing_first_run_call() {
        let lines = [
            decided("aml-score:1", OTHER),
            decided("aml-score:2", RESUMED),
        ];
        assert!(matches!(
            verdict(&killed_and_resumed(), &lines),
            CallLogVerdict::Violated(CallLogViolation::OutsideLineage { .. })
        ));
    }

    #[test]
    fn a_missing_log_holds_no_calls() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("never-created.jsonl");
        let lineage = Lineage::of(&killed_and_resumed()).expect("one lineage");
        let judgment = judge(&path, &lineage).expect("the missing log is judged");
        assert_eq!(judgment.calls, 0);
        assert_eq!(judgment.verdict, CallLogVerdict::Uncertified);
        let check = CallLogCheck::Judged {
            first_run: FIRST.to_string(),
            logs: vec![judgment],
        };
        assert_eq!(
            render_call_log_check(&check),
            format!(
                "uncertified: {} does not exist, so it holds no calls from {FIRST}, which started \
                 this lineage.\n  That run may never have called this system, or the ledger was \
                 cleared after it. A resume never clears the ledger.",
                path.display()
            )
        );
    }

    #[test]
    fn manifests_from_two_lineages_are_refused_before_any_log() {
        let refusal = Lineage::of(&[manifest(FIRST, FIRST, false), manifest(OTHER, OTHER, false)])
            .err()
            .expect("two lineages are refused");
        assert_eq!(
            render_call_log_check(&CallLogCheck::Refused(refusal)),
            format!(
                "error: the manifests belong to 2 lineages, started by {FIRST} and {OTHER}. List \
                 the runs of one lineage."
            )
        );
        assert!(matches!(
            Lineage::of(&[]),
            Err(RefusalReason::NoRunDirectories)
        ));
    }

    #[test]
    fn a_damaged_line_is_an_operational_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("calls.jsonl");
        std::fs::write(
            &path,
            format!("{}\nnot json\n", decided("aml-score:1", FIRST)),
        )
        .expect("write the log");
        let lineage = Lineage::of(&killed_and_resumed()).expect("one lineage");
        let error = judge(&path, &lineage).expect_err("the damaged line fails the check");
        assert!(
            error
                .to_string()
                .starts_with(&format!("call log {} line 2: ", path.display())),
            "{error}"
        );
    }

    #[test]
    fn verdicts_render_one_line_each_and_set_the_exit_code() {
        let judgment = |verdict, calls, replayed| CallLogJudgment {
            path: PathBuf::from("target/aml-risk-calls.jsonl"),
            calls,
            replayed_by_resumes: replayed,
            verdict,
        };
        let passed = judgment(CallLogVerdict::Passed, 279, 1);
        let repeats = judgment(
            CallLogVerdict::Violated(CallLogViolation::TooManyRepeats { replayed: 250000 }),
            500000,
            250000,
        );
        let twice = judgment(
            CallLogVerdict::Violated(CallLogViolation::DecidedTwice {
                key: "aml-score:2817341".to_string(),
                first: FIRST.to_string(),
                second: RESUMED.to_string(),
            }),
            2,
            0,
        );
        let outside = judgment(
            CallLogVerdict::Violated(CallLogViolation::OutsideLineage {
                flow_id: OTHER.to_string(),
                calls: 500000,
            }),
            500000,
            0,
        );
        let check = |logs: Vec<CallLogJudgment>| CallLogCheck::Judged {
            first_run: FIRST.to_string(),
            logs,
        };
        assert_eq!(
            render_call_log_check(&check(vec![
                passed.clone(),
                repeats.clone(),
                twice,
                outside,
            ])),
            [
                "ok: target/aml-risk-calls.jsonl holds 279 calls from this lineage, 1 repeated by a resume.".to_string(),
                "error: target/aml-risk-calls.jsonl holds 250000 calls repeated by resumes; each resume may repeat only the call its kill cut off.".to_string(),
                format!("error: aml-score:2817341 was decided twice in this lineage ({FIRST}, {RESUMED})."),
                format!("error: target/aml-risk-calls.jsonl holds 500000 calls from {OTHER}, which is not part of the lineage started by {FIRST}. List every run of the lineage. A fresh run starts with an empty ledger."),
            ]
            .join("\n")
        );

        let uncertified = judgment(CallLogVerdict::Uncertified, 0, 0);
        assert_eq!(check(vec![passed.clone()]).exit_code(), 0);
        assert_eq!(check(vec![passed, uncertified.clone()]).exit_code(), 2);
        assert_eq!(check(vec![repeats, uncertified]).exit_code(), 1);
        assert_eq!(
            CallLogCheck::Refused(RefusalReason::NoRunDirectories).exit_code(),
            3
        );
    }
}
