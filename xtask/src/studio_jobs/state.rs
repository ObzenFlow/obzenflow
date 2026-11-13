// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{STATE_DIR, STATE_FILE};
use crate::{error, Result};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub(super) struct JobState {
    pub(super) job_id: String,
    pub(super) example: String,
    pub(super) flow_name: String,
    pub(super) port: u16,
    pub(super) pid: u32,
    pub(super) config: PathBuf,
    pub(super) log: PathBuf,
}

pub(super) fn write_state(root: &Path, states: &[JobState]) -> Result<()> {
    fs::create_dir_all(root.join(STATE_DIR))?;
    let mut file = File::create(root.join(STATE_FILE))?;
    writeln!(file, "# obzenflow xtask studio-jobs v1")?;
    for state in states {
        writeln!(
            file,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            state.job_id,
            state.example,
            state.flow_name,
            state.port,
            state.pid,
            state.config.display(),
            state.log.display()
        )?;
    }
    Ok(())
}

pub(super) fn read_state(root: &Path) -> Result<Vec<JobState>> {
    let path = root.join(STATE_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(path)?;
    let mut states = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        states.push(parse_state_line(line, index + 1)?);
    }
    Ok(states)
}

fn parse_state_line(line: &str, line_number: usize) -> Result<JobState> {
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() != 7 {
        return Err(error(format!(
            "{STATE_FILE}:{line_number}: expected 7 tab-separated fields, got {}",
            fields.len()
        )));
    }

    Ok(JobState {
        job_id: fields[0].to_string(),
        example: fields[1].to_string(),
        flow_name: fields[2].to_string(),
        port: fields[3].parse()?,
        pid: fields[4].parse()?,
        config: PathBuf::from(fields[5]),
        log: PathBuf::from(fields[6]),
    })
}

pub(super) fn remove_state_file(root: &Path) -> Result<()> {
    let path = root.join(STATE_FILE);
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}
