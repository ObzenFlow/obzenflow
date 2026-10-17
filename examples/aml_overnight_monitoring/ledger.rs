// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::schema::FlowId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallResult {
    Scored,
    Opened,
    Replayed,
}

pub trait LedgerAnswer: Clone + Serialize + DeserializeOwned {
    const DECIDED: CallResult;
}

#[derive(Serialize, Deserialize)]
#[serde(bound(serialize = "A: Serialize", deserialize = "A: DeserializeOwned"))]
struct LedgerLine<A> {
    key: String,
    result: CallResult,
    flow_id: String,
    #[serde(flatten)]
    answer: Option<A>,
}

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error("ledger {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("ledger {} line {line}: {source}", path.display())]
    Line {
        path: PathBuf,
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("ledger {} line {line}: {result:?} does not match the answer it holds", path.display())]
    Shape {
        path: PathBuf,
        line: usize,
        result: CallResult,
    },
}

pub struct Ledger<A> {
    file: File,
    answers: HashMap<String, A>,
}

impl<A: LedgerAnswer> Ledger<A> {
    pub fn open(path: &Path) -> Result<Self, LedgerError> {
        let io = |source| LedgerError::Io {
            path: path.to_path_buf(),
            source,
        };
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(io)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .map_err(io)?;
        let mut text = String::new();
        file.read_to_string(&mut text).map_err(io)?;

        let mut answers = HashMap::new();
        for (index, text) in text.lines().enumerate() {
            let line: LedgerLine<A> =
                serde_json::from_str(text).map_err(|source| LedgerError::Line {
                    path: path.to_path_buf(),
                    line: index + 1,
                    source,
                })?;
            match (line.result, line.answer) {
                (result, Some(answer)) if result == A::DECIDED => {
                    answers.entry(line.key).or_insert(answer);
                }
                (CallResult::Replayed, None) => {}
                (result, _) => {
                    return Err(LedgerError::Shape {
                        path: path.to_path_buf(),
                        line: index + 1,
                        result,
                    })
                }
            }
        }
        Ok(Self { file, answers })
    }

    pub fn first_answer(&self, key: &str) -> Option<&A> {
        self.answers.get(key)
    }

    pub fn answers(&self) -> impl Iterator<Item = &A> {
        self.answers.values()
    }

    pub fn record_first(&mut self, key: &str, flow_id: FlowId, answer: A) -> std::io::Result<()> {
        self.append(&LedgerLine {
            key: key.to_owned(),
            result: A::DECIDED,
            flow_id: flow_id.to_string(),
            answer: Some(answer.clone()),
        })?;
        self.answers.insert(key.to_owned(), answer);
        Ok(())
    }

    pub fn record_repeat(&mut self, key: &str, flow_id: FlowId) -> std::io::Result<()> {
        self.append(&LedgerLine {
            key: key.to_owned(),
            result: CallResult::Replayed,
            flow_id: flow_id.to_string(),
            answer: None,
        })
    }

    fn append(&mut self, line: &LedgerLine<A>) -> std::io::Result<()> {
        let mut bytes = serde_json::to_vec(line)?;
        bytes.push(b'\n');
        self.file.write_all(&bytes)
    }
}
