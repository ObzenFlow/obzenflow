// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::{
    DEVELOPMENT_SESSION, DEVELOPMENT_STATE_ROOT, SESSION_OVERRIDE_ENV, SESSION_ROOT, STATE_FILE,
};
use crate::{error, Result};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

const STATE_HEADER: &str = "# obzenflow xtask postgres v3";

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
    pub(super) volume: Option<String>,
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
            validate_run_id(value)?;
            Ok(DevelopmentIdentity {
                directory: root.join(SESSION_ROOT).join(format!("persistent-{value}")),
                project: format!("obzenflow-persistent-{value}"),
            })
        }
        None => Ok(DevelopmentIdentity {
            directory: root.join(DEVELOPMENT_STATE_ROOT).join(DEVELOPMENT_SESSION),
            project: development_project(root),
        }),
    }
}

pub(super) fn test_identity(root: &Path, run_id: &str) -> Result<TestIdentity> {
    validate_run_id(run_id)?;
    Ok(TestIdentity {
        directory: root.join(SESSION_ROOT).join(run_id),
        project: format!("obzenflow-test-{run_id}"),
        run_id: run_id.to_string(),
    })
}

pub(super) fn create_session_directory(directory: &Path) -> Result<()> {
    if directory.exists() {
        return Err(error(format!(
            "refusing to reuse existing PostgreSQL session directory {}",
            directory.display()
        )));
    }
    let parent = directory
        .parent()
        .ok_or_else(|| error("PostgreSQL session directory has no parent"))?;
    fs::create_dir_all(parent)?;
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    }
    require_private_directory(directory)
}

pub(super) fn new_development(identity: &DevelopmentIdentity) -> SessionState {
    SessionState {
        project: identity.project.clone(),
        run_id: unique_run_id(),
        port: 0,
        mode: SessionMode::Development,
        volume: None,
    }
}

pub(super) fn new_test(identity: &TestIdentity) -> SessionState {
    SessionState {
        project: identity.project.clone(),
        run_id: identity.run_id.clone(),
        port: 0,
        mode: SessionMode::Test,
        volume: None,
    }
}

pub(super) fn expected_volume(project: &str) -> String {
    format!("{project}_postgres-data")
}

pub(super) fn write(path: &Path, state: &SessionState) -> Result<()> {
    validate_state(state)?;
    let directory = path
        .parent()
        .ok_or_else(|| error("PostgreSQL state file has no parent directory"))?;
    require_private_directory(directory)?;
    let temporary = directory.join(format!(".state-{}.tmp", unique_run_id()));
    let write_result = (|| {
        let mut file = private_file_create_new(&temporary)?;
        writeln!(file, "{STATE_HEADER}")?;
        writeln!(file, "project\t{}", state.project)?;
        writeln!(file, "run_id\t{}", state.run_id)?;
        writeln!(file, "port\t{}", state.port)?;
        writeln!(file, "mode\t{}", state.mode.as_str())?;
        if let Some(volume) = &state.volume {
            writeln!(file, "volume\t{volume}")?;
        }
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if write_result.is_err() && temporary.exists() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

pub(super) fn read(path: &Path) -> Result<SessionState> {
    require_private_regular_file(path)?;
    let contents = fs::read_to_string(path)?;
    let mut lines = contents.lines();
    if lines.next() != Some(STATE_HEADER) {
        return Err(error("unsupported PostgreSQL session state version"));
    }
    let mut project = None;
    let mut run_id = None;
    let mut port = None;
    let mut mode = None;
    let mut volume = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once('\t')
            .ok_or_else(|| error("invalid PostgreSQL session state"))?;
        match key {
            "project" => set_once(&mut project, value)?,
            "run_id" => set_once(&mut run_id, value)?,
            "port" => {
                if port.replace(value.parse::<u16>()?).is_some() {
                    return Err(error("duplicate PostgreSQL session state field"));
                }
            }
            "mode" => {
                if mode.replace(SessionMode::parse(value)?).is_some() {
                    return Err(error("duplicate PostgreSQL session state field"));
                }
            }
            "volume" => set_once(&mut volume, value)?,
            _ => return Err(error("invalid PostgreSQL session state field")),
        }
    }
    let state = SessionState {
        project: project.ok_or_else(|| error("PostgreSQL session project is missing"))?,
        run_id: run_id.ok_or_else(|| error("PostgreSQL session run id is missing"))?,
        port: port.ok_or_else(|| error("PostgreSQL session port is missing"))?,
        mode: mode.ok_or_else(|| error("PostgreSQL session mode is missing"))?,
        volume,
    };
    validate_state(&state)?;
    Ok(state)
}

pub(super) fn require_ready(state: &SessionState) -> Result<()> {
    if state.port == 0 || state.volume.is_none() {
        Err(error(
            "PostgreSQL session setup was interrupted while provisional; reset it with `cargo xtask postgres down --volumes`",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn require_development_authority(
    state: &SessionState,
    identity: &DevelopmentIdentity,
) -> Result<()> {
    if state.mode == SessionMode::Development
        && state.project == identity.project
        && valid_project(&state.project)
        && state.run_id.len() == 32
    {
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

pub(super) fn record_or_verify_volume(state: &mut SessionState, actual: &str) -> Result<()> {
    let derived = expected_volume(&state.project);
    if actual != derived
        || state
            .volume
            .as_deref()
            .is_some_and(|stored| stored != actual)
    {
        return Err(error(format!(
            "PostgreSQL volume changed unexpectedly: expected {derived}, found {actual}; no replacement volume was adopted"
        )));
    }
    state.volume = Some(actual.to_string());
    Ok(())
}

pub(super) fn require_owned_directory(root: &Path, directory: &Path) -> Result<()> {
    require_private_directory(directory).map_err(|_| {
        error("refusing PostgreSQL cleanup because the session path is not an owned directory")
    })?;
    let canonical_directory = directory
        .canonicalize()
        .map_err(|_| error("PostgreSQL session directory is unavailable"))?;
    let allowed_parents = [root.join(SESSION_ROOT), root.join(DEVELOPMENT_STATE_ROOT)]
        .into_iter()
        .filter_map(|path| path.canonicalize().ok())
        .collect::<Vec<_>>();
    if !allowed_parents
        .iter()
        .any(|parent| canonical_directory.parent() == Some(parent.as_path()))
    {
        return Err(error(
            "refusing PostgreSQL cleanup because the session path escaped its authority root",
        ));
    }
    require_private_regular_file(&directory.join(STATE_FILE)).map_err(|_| {
        error("refusing PostgreSQL cleanup because the session state is not an owned file")
    })
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
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if validate_run_id(&name).is_err() {
            continue;
        }
        let directory = entry.path();
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

fn validate_state(state: &SessionState) -> Result<()> {
    if !valid_project(&state.project) {
        return Err(error("invalid PostgreSQL session project"));
    }
    validate_run_id(&state.run_id)?;
    if let Some(volume) = &state.volume {
        if !valid_volume(volume) || volume != &expected_volume(&state.project) {
            return Err(error("invalid PostgreSQL session volume"));
        }
    }
    Ok(())
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

fn valid_project(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn valid_volume(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn set_once(slot: &mut Option<String>, value: &str) -> Result<()> {
    if slot.replace(value.to_string()).is_some() {
        Err(error("duplicate PostgreSQL session state field"))
    } else {
        Ok(())
    }
}

fn private_file_create_new(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

fn require_private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| error("PostgreSQL session directory is unavailable"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(error("PostgreSQL session path is not a regular directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(error(
                "PostgreSQL session directory permissions must be 0700",
            ));
        }
    }
    Ok(())
}

fn require_private_regular_file(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| error("PostgreSQL session state is unavailable"))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(error("PostgreSQL session state is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(error("PostgreSQL session state permissions must be 0600"));
        }
    }
    Ok(())
}

fn checkout_fingerprint(root: &Path) -> String {
    let canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    stable_fingerprint(canonical.to_string_lossy().as_bytes())
}

fn stable_fingerprint(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn development_project(root: &Path) -> String {
    format!("obzenflow-postgres-v3-{}", checkout_fingerprint(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn private_directory(root: &Path, name: &str) -> PathBuf {
        let directory = root.join(name);
        create_session_directory(&directory).expect("create private session directory");
        directory
    }

    #[test]
    fn state_round_trips_without_credentials() {
        let root = env::temp_dir().join(format!("obzenflow-state-{}", unique_run_id()));
        fs::create_dir_all(&root).expect("create state root");
        let directory = private_directory(&root, "session");
        let path = directory.join(STATE_FILE);
        let project = "obzenflow-test-state".to_string();
        let expected = SessionState {
            project: project.clone(),
            run_id: unique_run_id(),
            port: 15432,
            mode: SessionMode::Test,
            volume: Some(expected_volume(&project)),
        };
        write(&path, &expected).expect("write state");
        let contents = fs::read_to_string(&path).expect("read state");
        assert!(!contents.contains("postgres://"));
        assert!(!contents.contains("password"));
        assert_eq!(read(&path).expect("restore state"), expected);
        fs::remove_dir_all(root).expect("remove state fixture");
    }

    #[test]
    fn provisional_state_requires_an_explicit_reset() {
        let identity = TestIdentity {
            directory: PathBuf::from("unused"),
            project: "obzenflow-test-provisional".to_string(),
            run_id: unique_run_id(),
        };
        let mut state = new_test(&identity);
        assert!(require_ready(&state).is_err());
        state.port = 15432;
        let volume = expected_volume(&state.project);
        record_or_verify_volume(&mut state, &volume).expect("record exact volume");
        require_ready(&state).expect("ready state");
    }

    #[test]
    fn cleanup_authority_is_bounded_to_one_session_directory() {
        let root = env::temp_dir().join(format!("obzenflow-authority-{}", unique_run_id()));
        let identity = test_identity(&root, &unique_run_id()).expect("derive test identity");
        create_session_directory(&identity.directory).expect("create session directory");
        write(&identity.directory.join(STATE_FILE), &new_test(&identity))
            .expect("write owned state");
        require_owned_directory(&root, &identity.directory).expect("accept owned directory");
        assert!(require_owned_directory(&root, &root).is_err());
        remove_owned_directory(&root, &identity.directory).expect("remove owned directory");
        fs::remove_dir_all(root).expect("remove authority root");
    }

    #[test]
    fn development_project_fingerprint_is_stable_and_versioned() {
        assert_eq!(stable_fingerprint(b""), "cbf29ce484222325");
        assert_eq!(stable_fingerprint(b"obzenflow"), "1b2e029ee10c82e9");
        assert!(
            development_project(Path::new("/tmp/obzenflow")).starts_with("obzenflow-postgres-v3-")
        );
    }
}
