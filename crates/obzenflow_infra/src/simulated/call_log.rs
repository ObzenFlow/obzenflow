// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::FlowId;
use obzenflow_runtime::effects::IdempotencyKey;
use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

/// A simulated system's saved responses, one line per physical call.
///
/// Each key's decided response is the system's memory: every later call for
/// the key is answered with it and logged as `replayed`. Each line is one write
/// on an unbuffered file, so the log outlives a killed process. The operator
/// empties the log before each fresh run and never before a resume.
pub struct CallLog<A> {
    path: PathBuf,
    state: Mutex<State<A>>,
}

struct State<A> {
    file: File,
    decided: HashMap<String, A>,
}

/// Why a call log could not open or record a call.
#[derive(Debug, thiserror::Error)]
pub enum CallLogError {
    /// The log's file could not be created, read or written.
    #[error("call log {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A line of the log does not parse.
    #[error("call log {} line {line}: {message}", path.display())]
    Line {
        path: PathBuf,
        line: usize,
        message: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CallLogLine<A, F> {
    Decided {
        key: String,
        flow_id: String,
        response: A,
    },
    Failed {
        key: String,
        flow_id: String,
        error: F,
    },
    Replayed {
        key: String,
        flow_id: String,
    },
}

#[derive(Serialize)]
enum Never {}

impl<A> CallLog<A>
where
    A: Clone + Serialize + DeserializeOwned,
{
    /// Opens the log at `path`, creating the file and its directory when they
    /// are missing, and rebuilds each key's decided response from its lines.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be opened or read, and names the line number
    /// and the parser's message for the first line that does not parse.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CallLogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|source| io_error(&path, source))?;
        }
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        Self::load(path, file)
    }

    fn load(path: PathBuf, file: File) -> Result<Self, CallLogError> {
        let mut decided = HashMap::new();
        read_lines(&path, &file, |line: CallLogLine<A, IgnoredAny>| {
            if let CallLogLine::Decided { key, response, .. } = line {
                decided.entry(key).or_insert(response);
            }
        })?;
        Ok(Self {
            path,
            state: Mutex::new(State { file, decided }),
        })
    }

    /// Answers a call for `key` from memory, or with the response `decide`
    /// returns.
    ///
    /// A decided key is logged as `replayed` and answered with its decided
    /// response, without calling `decide`. A new key calls `decide` with the
    /// number of keys decided so far, and its response is logged as `decided`
    /// before it is remembered.
    ///
    /// # Errors
    ///
    /// Fails when the line cannot be written, and then remembers nothing.
    pub fn answer(
        &self,
        key: &IdempotencyKey,
        flow_id: FlowId,
        decide: impl FnOnce(u64) -> A,
    ) -> Result<A, CallLogError> {
        let Ok(response) = self.call(key, flow_id, |decided| Ok::<A, Never>(decide(decided)))?;
        Ok(response)
    }

    /// Answers like [`CallLog::answer`] with a decision that may fail.
    ///
    /// A failure is logged as `failed` and returned, and the key stays
    /// undecided, so the next attempt decides again.
    ///
    /// # Errors
    ///
    /// Returns the decision's failure, or the log's error when the line cannot
    /// be written.
    pub fn try_answer<F, E>(
        &self,
        key: &IdempotencyKey,
        flow_id: FlowId,
        decide: impl FnOnce(u64) -> Result<A, F>,
    ) -> Result<A, E>
    where
        F: Serialize,
        E: From<F> + From<CallLogError>,
    {
        let answer = self.call(key, flow_id, decide)?;
        answer.map_err(E::from)
    }

    fn call<F: Serialize>(
        &self,
        key: &IdempotencyKey,
        flow_id: FlowId,
        decide: impl FnOnce(u64) -> Result<A, F>,
    ) -> Result<Result<A, F>, CallLogError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let key = key.0.clone();
        let flow_id = flow_id.to_string();
        if let Some(response) = state.decided.get(&key).cloned() {
            state.append(
                &self.path,
                &CallLogLine::<&A, &F>::Replayed { key, flow_id },
            )?;
            return Ok(Ok(response));
        }
        match decide(state.decided.len() as u64) {
            Ok(response) => {
                state.append(
                    &self.path,
                    &CallLogLine::<&A, &F>::Decided {
                        key: key.clone(),
                        flow_id,
                        response: &response,
                    },
                )?;
                state.decided.insert(key, response.clone());
                Ok(Ok(response))
            }
            Err(failure) => {
                state.append(
                    &self.path,
                    &CallLogLine::<&A, &F>::Failed {
                        key,
                        flow_id,
                        error: &failure,
                    },
                )?;
                Ok(Err(failure))
            }
        }
    }
}

impl<A> fmt::Debug for CallLog<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallLog")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl<A> State<A> {
    fn append(&mut self, path: &Path, line: &impl Serialize) -> Result<(), CallLogError> {
        let mut bytes = serde_json::to_vec(line).map_err(|error| io_error(path, error.into()))?;
        bytes.push(b'\n');
        self.file
            .write_all(&bytes)
            .map_err(|source| io_error(path, source))
    }
}

/// Reads every line of the log at `path` through the call log's own line
/// type, stopping at the first line that does not parse.
pub(crate) fn read_lines<A, F>(
    path: &Path,
    reader: impl Read,
    mut visit: impl FnMut(CallLogLine<A, F>),
) -> Result<(), CallLogError>
where
    A: DeserializeOwned,
    F: DeserializeOwned,
{
    for (index, text) in BufReader::new(reader).lines().enumerate() {
        let text = text.map_err(|source| io_error(path, source))?;
        let line = serde_json::from_str(&text).map_err(|error| CallLogError::Line {
            path: path.to_path_buf(),
            line: index + 1,
            message: parser_message(&error),
        })?;
        visit(line);
    }
    Ok(())
}

fn parser_message(error: &serde_json::Error) -> String {
    let message = error.to_string();
    let position = format!(" at line {} column {}", error.line(), error.column());
    match message.strip_suffix(&position) {
        Some(stripped) => stripped.to_owned(),
        None => message,
    }
}

fn io_error(path: &Path, source: std::io::Error) -> CallLogError {
    CallLogError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::{self, Deserializer};
    use std::cell::Cell;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "verdict", rename_all = "snake_case")]
    enum Verdict {
        Cleared {
            rules_version: String,
        },
        Flagged {
            reasons: Reasons,
            rules_version: String,
        },
    }

    #[derive(Clone, Debug, PartialEq, Serialize)]
    struct Reasons(Vec<String>);

    impl<'de> Deserialize<'de> for Reasons {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            let reasons = Vec::<String>::deserialize(deserializer)?;
            if reasons.is_empty() {
                return Err(de::Error::custom("a flag needs at least one reason"));
            }
            Ok(Self(reasons))
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize)]
    #[serde(tag = "reason", rename_all = "snake_case")]
    enum Failure {
        TimedOut,
    }

    #[derive(Debug, PartialEq)]
    enum PortError {
        TimedOut,
        Log(String),
    }

    impl From<Failure> for PortError {
        fn from(failure: Failure) -> Self {
            match failure {
                Failure::TimedOut => Self::TimedOut,
            }
        }
    }

    impl From<CallLogError> for PortError {
        fn from(error: CallLogError) -> Self {
            Self::Log(error.to_string())
        }
    }

    fn key(name: &str) -> IdempotencyKey {
        IdempotencyKey(name.to_owned())
    }

    fn flagged() -> Verdict {
        Verdict::Flagged {
            reasons: Reasons(vec!["format_switch".to_owned()]),
            rules_version: "sim-2026.10".to_owned(),
        }
    }

    fn cleared() -> Verdict {
        Verdict::Cleared {
            rules_version: "sim-2026.10".to_owned(),
        }
    }

    fn lines(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .expect("the log is readable")
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is JSON"))
            .collect()
    }

    fn open_error(path: &Path, text: &str) -> String {
        std::fs::write(path, text).expect("write the log");
        CallLog::<Verdict>::open(path)
            .expect_err("the log refuses the line")
            .to_string()
    }

    #[test]
    fn a_repeated_key_is_answered_replayed_with_its_decided_response() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("calls").join("risk.jsonl");
        let log = CallLog::open(&path).expect("the log opens");
        let run = FlowId::new();

        let first = log.answer(&key("aml-score:8"), run, |_| flagged()).unwrap();
        let repeat = log
            .answer(&key("aml-score:8"), run, |_| {
                panic!("a decided key never decides again")
            })
            .unwrap();
        assert_eq!(first, flagged());
        assert_eq!(repeat, first);
        assert_eq!(
            lines(&path),
            vec![
                serde_json::json!({
                    "result": "decided",
                    "key": "aml-score:8",
                    "flow_id": run.to_string(),
                    "response": {
                        "verdict": "flagged",
                        "reasons": ["format_switch"],
                        "rules_version": "sim-2026.10",
                    },
                }),
                serde_json::json!({
                    "result": "replayed",
                    "key": "aml-score:8",
                    "flow_id": run.to_string(),
                }),
            ]
        );
    }

    #[test]
    fn a_reopened_log_answers_a_later_runs_call_from_memory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("risk.jsonl");
        let killed = FlowId::new();
        CallLog::open(&path)
            .expect("the log opens")
            .answer(&key("aml-score:9"), killed, |_| flagged())
            .unwrap();

        let resumed = FlowId::new();
        let reopened = CallLog::<Verdict>::open(&path).expect("the log reopens");
        let repeat = reopened
            .answer(&key("aml-score:9"), resumed, |_| cleared())
            .unwrap();
        assert_eq!(repeat, flagged());
        let new = reopened
            .answer(&key("aml-score:10"), resumed, |_| cleared())
            .unwrap();
        assert_eq!(new, cleared());

        let written: Vec<_> = lines(&path)
            .iter()
            .map(|line| {
                (
                    line["result"].as_str().map(str::to_owned),
                    line["key"].as_str().map(str::to_owned),
                    line["flow_id"].as_str().map(str::to_owned),
                )
            })
            .collect();
        let line = |result: &str, key: &str, run: FlowId| {
            (
                Some(result.to_owned()),
                Some(key.to_owned()),
                Some(run.to_string()),
            )
        };
        assert_eq!(
            written,
            vec![
                line("decided", "aml-score:9", killed),
                line("replayed", "aml-score:9", resumed),
                line("decided", "aml-score:10", resumed),
            ]
        );
    }

    #[test]
    fn new_keys_are_numbered_in_call_order_from_the_decided_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cases.jsonl");
        let run = FlowId::new();
        let log = CallLog::open(&path).expect("the log opens");
        let number = |count: u64| count + 1;
        assert_eq!(log.answer(&key("aml-case:5"), run, number).unwrap(), 1);
        assert_eq!(log.answer(&key("aml-case:3"), run, number).unwrap(), 2);
        assert_eq!(log.answer(&key("aml-case:5"), run, number).unwrap(), 1);
        let failed: Result<u64, PortError> =
            log.try_answer(&key("aml-case:7"), run, |_| Err(Failure::TimedOut));
        assert_eq!(failed, Err(PortError::TimedOut));
        drop(log);

        let reopened = CallLog::<u64>::open(&path).expect("the log reopens");
        assert_eq!(
            reopened
                .answer(&key("aml-case:3"), FlowId::new(), number)
                .unwrap(),
            2
        );
        assert_eq!(
            reopened
                .answer(&key("aml-case:7"), FlowId::new(), number)
                .unwrap(),
            3
        );
    }

    #[test]
    fn a_failed_attempt_decides_nothing_and_the_next_attempt_decides() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gateway.jsonl");
        let run = FlowId::new();
        let log = CallLog::open(&path).expect("the log opens");
        let outage: Result<String, PortError> =
            log.try_answer(&key("payment-authorize:web-1"), run, |_| {
                Err(Failure::TimedOut)
            });
        assert_eq!(outage, Err(PortError::TimedOut));
        let charged: Result<String, PortError> =
            log.try_answer(&key("payment-authorize:web-1"), run, |decided| {
                assert_eq!(decided, 0, "a failed attempt is not counted");
                Ok::<_, Failure>("AUTH-web-1".to_owned())
            });
        assert_eq!(charged, Ok("AUTH-web-1".to_owned()));
        assert_eq!(
            lines(&path),
            vec![
                serde_json::json!({
                    "result": "failed",
                    "key": "payment-authorize:web-1",
                    "flow_id": run.to_string(),
                    "error": {"reason": "timed_out"},
                }),
                serde_json::json!({
                    "result": "decided",
                    "key": "payment-authorize:web-1",
                    "flow_id": run.to_string(),
                    "response": "AUTH-web-1",
                }),
            ]
        );
        drop(log);

        let reopened = CallLog::<String>::open(&path).expect("a failed line reads back");
        let repeat = reopened
            .answer(&key("payment-authorize:web-1"), run, |_| {
                panic!("the decided charge is remembered")
            })
            .unwrap();
        assert_eq!(repeat, "AUTH-web-1");
    }

    #[test]
    fn a_failed_write_remembers_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("risk.jsonl");
        std::fs::write(&path, "").expect("create the log");
        let read_only = File::open(&path).expect("open the log read-only");
        let log = CallLog::<Verdict>::load(path.clone(), read_only).expect("the log loads");
        let decisions = Cell::new(0);
        for _ in 0..2 {
            let error = log
                .answer(&key("aml-score:1"), FlowId::new(), |_| {
                    decisions.set(decisions.get() + 1);
                    flagged()
                })
                .expect_err("the write fails");
            assert!(matches!(error, CallLogError::Io { .. }), "{error}");
        }
        assert_eq!(decisions.get(), 2, "the key was never remembered");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
    }

    #[test]
    fn a_deciding_line_is_on_disk_when_answer_returns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gateway.jsonl");
        let log = CallLog::open(&path).expect("the log opens");
        let run = FlowId::new();
        log.answer(&key("payment-authorize:S-12"), run, |_| cleared())
            .unwrap();
        let on_disk = lines(&path);
        assert_eq!(
            on_disk.len(),
            1,
            "the line is written before answer returns"
        );
        assert_eq!(on_disk[0]["result"], "decided");
        assert_eq!(on_disk[0]["key"], "payment-authorize:S-12");
    }

    #[test]
    fn a_damaged_line_fails_the_open_with_its_file_line_and_parser_message() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("risk.jsonl");
        let valid = r#"{"result":"decided","key":"aml-score:1","flow_id":"flow_1","response":{"verdict":"cleared","rules_version":"sim-2026.10"}}"#;
        let damaged = r#"{"result":"decided","key":"aml-score:2","flow_id":"flow_1","response":{"verdict":"flagged","reasons":[],"rules_version":"sim-2026.10"}}"#;
        assert_eq!(
            open_error(&path, &format!("{valid}\n{damaged}\n")),
            format!(
                "call log {} line 2: a flag needs at least one reason",
                path.display()
            )
        );
    }

    #[test]
    fn no_decided_line_parses_without_its_response_and_no_replayed_line_with_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("risk.jsonl");
        let without = open_error(
            &path,
            "{\"result\":\"decided\",\"key\":\"aml-score:1\",\"flow_id\":\"flow_1\"}\n",
        );
        assert!(
            without.contains("line 1: missing field `response`"),
            "{without}"
        );

        let with = open_error(
            &path,
            "{\"result\":\"replayed\",\"key\":\"aml-score:1\",\"flow_id\":\"flow_1\",\"response\":{\"verdict\":\"cleared\",\"rules_version\":\"sim-2026.10\"}}\n",
        );
        assert!(with.contains("line 1: unknown field `response`"), "{with}");

        let old = open_error(
            &path,
            "{\"key\":\"aml-score:1\",\"result\":\"scored\",\"flow_id\":\"flow_1\",\"verdict\":\"cleared\",\"rules_version\":\"sim-2026.10\"}\n",
        );
        assert!(old.contains("line 1: unknown variant `scored`"), "{old}");
    }
}
