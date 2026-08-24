// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::{DEVELOPMENT_SESSION, SESSION_OVERRIDE_ENV, SESSION_ROOT, STATE_FILE};
use crate::{error, Result};
use std::{
    env,
    fs::{self, File},
    hash::{Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SessionMode {
    Development,
    Test,
}

impl SessionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Test => "test",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "development" => Ok(Self::Development),
            "test" => Ok(Self::Test),
            _ => Err(error("invalid PostgreSQL session mode")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SessionState {
    pub(super) project: String,
    pub(super) run_id: String,
    pub(super) port: u16,
    pub(super) mode: SessionMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DevelopmentIdentity {
    pub(super) directory: PathBuf,
    pub(super) project: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TestIdentity {
    pub(super) directory: PathBuf,
    pub(super) project: String,
    pub(super) run_id: String,
}

pub(super) fn unique_run_id() -> String {
    Uuid::new_v4().simple().to_string()
}

pub(super) fn development_identity(root: &Path) -> Result<DevelopmentIdentity> {
    match env::var_os(SESSION_OVERRIDE_ENV) {
        Some(value) => {
            let value = value
                .to_str()
                .ok_or_else(|| error("invalid isolated PostgreSQL session identity"))?;
            isolated_development_identity(root, value)
        }
        None => Ok(DevelopmentIdentity {
            directory: root.join(SESSION_ROOT).join(DEVELOPMENT_SESSION),
            project: development_project(root),
        }),
    }
}

fn isolated_development_identity(root: &Path, token: &str) -> Result<DevelopmentIdentity> {
    validate_run_id(token)?;
    Ok(DevelopmentIdentity {
        directory: root.join(SESSION_ROOT).join(format!("persistent-{token}")),
        project: format!("obzenflow-persistent-{token}"),
    })
}

pub(super) fn test_identity(root: &Path, run_id: &str) -> Result<TestIdentity> {
    validate_run_id(run_id)?;
    Ok(TestIdentity {
        directory: root.join(SESSION_ROOT).join(run_id),
        project: format!("obzenflow-test-{run_id}"),
        run_id: run_id.to_string(),
    })
}

fn validate_run_id(run_id: &str) -> Result<()> {
    if run_id.len() == 32
        && run_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(error("invalid PostgreSQL session run id"))
    }
}

pub(super) fn write(path: &Path, state: &SessionState) -> Result<()> {
    let mut file = File::create(path)?;
    writeln!(file, "# obzenflow xtask postgres v1")?;
    writeln!(file, "project\t{}", state.project)?;
    writeln!(file, "run_id\t{}", state.run_id)?;
    writeln!(file, "port\t{}", state.port)?;
    writeln!(file, "mode\t{}", state.mode.as_str())?;
    Ok(())
}

pub(super) fn read(path: &Path) -> Result<SessionState> {
    let contents = fs::read_to_string(path)?;
    let mut project = None;
    let mut run_id = None;
    let mut port = None;
    let mut mode = None;
    for line in contents.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('\t')
            .ok_or_else(|| error("invalid PostgreSQL session state"))?;
        match key {
            "project" => project = Some(value.to_string()),
            "run_id" => run_id = Some(value.to_string()),
            "port" => port = Some(value.parse::<u16>()?),
            "mode" => mode = Some(SessionMode::parse(value)?),
            _ => return Err(error("invalid PostgreSQL session state field")),
        }
    }
    Ok(SessionState {
        project: project.ok_or_else(|| error("PostgreSQL session project is missing"))?,
        run_id: run_id.ok_or_else(|| error("PostgreSQL session run id is missing"))?,
        port: port.ok_or_else(|| error("PostgreSQL session port is missing"))?,
        mode: mode.ok_or_else(|| error("PostgreSQL session mode is missing"))?,
    })
}

pub(super) fn require_development_authority(
    state: &SessionState,
    identity: &DevelopmentIdentity,
) -> Result<()> {
    if state.mode == SessionMode::Development && state.project == identity.project {
        Ok(())
    } else {
        Err(error(
            "PostgreSQL development state does not match the selected project authority",
        ))
    }
}

pub(super) fn require_test_authority(state: &SessionState, identity: &TestIdentity) -> Result<()> {
    if state.mode == SessionMode::Test
        && state.project == identity.project
        && state.run_id == identity.run_id
    {
        Ok(())
    } else {
        Err(error(
            "PostgreSQL test state does not match its derived session authority",
        ))
    }
}

pub(super) fn require_owned_directory(root: &Path, directory: &Path) -> Result<()> {
    let session_root = root.join(SESSION_ROOT);
    let canonical_root = session_root
        .canonicalize()
        .map_err(|_| error("PostgreSQL session root is unavailable"))?;
    let metadata = fs::symlink_metadata(directory)
        .map_err(|_| error("PostgreSQL session directory is unavailable"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(error(
            "refusing PostgreSQL cleanup because the session path is not an owned directory",
        ));
    }
    let canonical_directory = directory
        .canonicalize()
        .map_err(|_| error("PostgreSQL session directory is unavailable"))?;
    if canonical_directory.parent() != Some(canonical_root.as_path()) {
        return Err(error(
            "refusing PostgreSQL cleanup because the session path escaped its authority root",
        ));
    }
    let state_metadata = fs::symlink_metadata(directory.join(STATE_FILE))
        .map_err(|_| error("PostgreSQL session state is unavailable"))?;
    if !state_metadata.is_file() || state_metadata.file_type().is_symlink() {
        return Err(error(
            "refusing PostgreSQL cleanup because the session state is not an owned file",
        ));
    }
    Ok(())
}

pub(super) fn remove_owned_directory(root: &Path, directory: &Path) -> Result<()> {
    require_owned_directory(root, directory)?;
    fs::remove_dir_all(directory)?;
    Ok(())
}

pub(super) fn test_sessions(root: &Path) -> Result<Vec<(PathBuf, SessionState)>> {
    let session_root = root.join(SESSION_ROOT);
    if !session_root.is_dir() {
        return Ok(Vec::new());
    }
    let mut sessions = Vec::new();
    for entry in fs::read_dir(session_root)? {
        let directory = entry?.path();
        let state_path = directory.join(STATE_FILE);
        if !state_path.is_file() {
            continue;
        }
        let state = read(&state_path)?;
        if state.mode == SessionMode::Test {
            sessions.push((directory, state));
        }
    }
    Ok(sessions)
}

fn development_project(root: &Path) -> String {
    let canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.hash(&mut hasher);
    format!("obzenflow-postgres-{:x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trips_without_credentials() {
        let directory = env::temp_dir().join(format!("obzenflow-state-{}", unique_run_id()));
        fs::create_dir_all(&directory).expect("create state directory");
        let path = directory.join(STATE_FILE);
        let expected = SessionState {
            project: "obzenflow-test-state".to_string(),
            run_id: unique_run_id(),
            port: 15432,
            mode: SessionMode::Test,
        };
        write(&path, &expected).expect("write state");
        let contents = fs::read_to_string(&path).expect("read state");
        assert!(!contents.contains("postgres://"));
        assert!(!contents.contains("password"));
        assert_eq!(read(&path).expect("restore state"), expected);
        fs::remove_dir_all(directory).expect("remove state directory");
    }

    #[test]
    fn cleanup_authority_is_bounded_to_one_session_directory() {
        let root = env::temp_dir().join(format!("obzenflow-authority-{}", unique_run_id()));
        let identity = test_identity(&root, &unique_run_id()).expect("derive test identity");
        fs::create_dir_all(&identity.directory).expect("create session directory");
        write(
            &identity.directory.join(STATE_FILE),
            &SessionState {
                project: identity.project.clone(),
                run_id: identity.run_id.clone(),
                port: 15432,
                mode: SessionMode::Test,
            },
        )
        .expect("write owned state");
        require_owned_directory(&root, &identity.directory).expect("accept owned directory");
        assert!(require_owned_directory(&root, &root).is_err());
        remove_owned_directory(&root, &identity.directory).expect("remove owned directory");
        fs::remove_dir_all(root).expect("remove authority root");
    }
}
