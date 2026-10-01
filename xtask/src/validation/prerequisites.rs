// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Operation-specific admission. A successful probe reserves no fixture resource
//! and cannot turn a subsequent execution failure into an environmental skip.
use super::{
    plan::{Lane, Policy, TestId},
    process,
};
use crate::{error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Capability {
    Ipv4LoopbackBind,
    Ipv4LoopbackConnect,
    Ipv6LoopbackBind,
    Ipv6LoopbackConnect,
    TempFiles,
    ArtifactFiles,
    TargetFiles,
    TempHardLinks,
    ArtifactHardLinks,
    TempModes,
    TempSymlinks,
    OwnedChild,
    OwnedChildSignals,
}

impl Capability {
    fn name(self) -> String {
        serde_json::to_value(self)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned()
    }

    fn operation(self) -> &'static str {
        match self {
            Self::Ipv4LoopbackBind | Self::Ipv6LoopbackBind => "bind TCP listener",
            Self::Ipv4LoopbackConnect | Self::Ipv6LoopbackConnect => {
                "bind, connect, accept and exchange TCP bytes"
            }
            Self::TempFiles | Self::ArtifactFiles | Self::TargetFiles => {
                "create, write, read, sync, rename and remove a file"
            }
            Self::TempHardLinks | Self::ArtifactHardLinks => "create and read a hard link",
            Self::TempModes => "set and inspect Unix file and directory modes",
            Self::TempSymlinks => "create and read a symbolic link",
            Self::OwnedChild => "spawn and join an owned child",
            Self::OwnedChildSignals => {
                "inspect, signal and reap owned children (SIGINT, SIGTERM, SIGKILL)"
            }
        }
    }

    fn scope(self, root: &Path, artifacts: &Path) -> String {
        match self {
            Self::Ipv4LoopbackBind | Self::Ipv4LoopbackConnect => "127.0.0.1:0".into(),
            Self::Ipv6LoopbackBind | Self::Ipv6LoopbackConnect => "[::1]:0".into(),
            Self::ArtifactFiles | Self::ArtifactHardLinks => artifacts.display().to_string(),
            Self::TargetFiles => root.join("target").display().to_string(),
            Self::OwnedChild | Self::OwnedChildSignals => {
                "children owned by this probe process".into()
            }
            _ => std::env::temp_dir().display().to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Requirement {
    pub(super) binary: String,
    pub(super) test: String,
    /// Inventory applicability, never a source from which capabilities are inferred.
    pub(super) lanes: Vec<Lane>,
    pub(super) capabilities: Vec<Capability>,
    pub(super) rationale: String,
}

impl Requirement {
    fn id(&self) -> TestId {
        TestId {
            binary: self.binary.clone(),
            test: self.test.clone(),
        }
    }
}

pub(super) fn validate(requirements: &[Requirement]) -> Result<()> {
    let mut ids = BTreeSet::new();
    for requirement in requirements {
        if !ids.insert(requirement.id())
            || requirement.binary.trim().is_empty()
            || requirement.test.trim().is_empty()
            || requirement.rationale.trim().is_empty()
            || requirement.capabilities.is_empty()
            || requirement
                .capabilities
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != requirement.capabilities.len()
            || requirement.lanes.is_empty()
            || requirement.lanes.iter().collect::<BTreeSet<_>>().len() != requirement.lanes.len()
            || requirement.lanes.iter().any(|lane| {
                !matches!(
                    lane,
                    Lane::Default
                        | Lane::ProductionFeatures
                        | Lane::TestSupport
                        | Lane::JournalFixtures
                )
            })
        {
            return Err(error(format!(
                "invalid or duplicate prerequisite declaration: {:?}",
                requirement.id()
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ProbeFailure {
    kind: String,
    os_code: Option<i32>,
    message: String,
}

impl From<io::Error> for ProbeFailure {
    fn from(error: io::Error) -> Self {
        Self {
            kind: format!("{:?}", error.kind()),
            os_code: process::os_error_code(&error),
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ProbeRecord {
    capability: Capability,
    operation: &'static str,
    scope: String,
    affected: BTreeSet<TestId>,
    duration_seconds: f64,
    completed: bool,
    failure: Option<ProbeFailure>,
    infrastructure_failure: Option<InfrastructureFailure>,
}

#[derive(Debug, Serialize)]
struct InfrastructureFailure {
    phase: String,
    executable: String,
    error: ProbeFailure,
}

enum ProbeOutcome {
    Completed(Option<ProbeFailure>),
    Incomplete(InfrastructureFailure),
}

#[derive(Debug, Serialize)]
pub(super) struct Admission {
    pub(super) required: BTreeSet<TestId>,
    pub(super) runnable: BTreeSet<TestId>,
    pub(super) blocked: BTreeSet<TestId>,
    probes: Vec<ProbeRecord>,
}

impl Admission {
    fn save(&self, directory: &Path) -> Result<()> {
        let pending = directory.join("prerequisites.pending.json");
        fs::write(&pending, serde_json::to_vec_pretty(self)?)?;
        fs::rename(pending, directory.join("prerequisites.json"))?;
        Ok(())
    }

    /// Preserve both facts when runnable tests fail and required tests are blocked.
    pub(super) fn finish(&self, directory: &Path, executed: Result<()>) -> Result<()> {
        fs::write(
            directory.join("coverage.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "required": self.required, "runnable": self.runnable, "blocked": self.blocked,
                "execution": if self.runnable.is_empty() { "not-run" } else if executed.is_ok() { "passed" }
                    else if executed.as_ref().unwrap_err().is::<super::CheckFailed>() { "failed" } else { "incomplete" },
                "execution_detail": executed.as_ref().err().map(ToString::to_string),
            }))?,
        )?;
        if self.blocked.is_empty() {
            executed
        } else {
            let incomplete = self
                .probes
                .iter()
                .filter(|record| !record.completed)
                .count();
            let denied = self
                .probes
                .iter()
                .filter(|record| record.failure.is_some())
                .count();
            Err(error(format!("{} required cases blocked: {incomplete} probes incomplete (capability unknown), {denied} completed probes reported unavailable capabilities; runnable execution: {}; see prerequisites.json, coverage.json and outcomes.json",
                self.blocked.len(), executed.err().map_or_else(
                    || if self.runnable.is_empty() { "not run".into() } else { "passed".into() },
                    |error| error.to_string()))))
        }
    }

    pub(super) fn exclusion_filter(&self) -> Option<String> {
        if self.blocked.is_empty() {
            return None;
        }
        // Nextest equality matchers use their own escaping, not shell quoting.
        // https://nexte.st/docs/filtersets/reference/#escape-sequences
        fn escape(name: &str) -> String {
            name.chars()
                .map(|ch| match ch {
                    '\\' => "\\\\".into(),
                    ')' => "\\)".into(),
                    ',' => "\\,".into(),
                    '\n' => "\\n".into(),
                    '\r' => "\\r".into(),
                    '\t' => "\\t".into(),
                    ch => ch.to_string(),
                })
                .collect()
        }
        Some(format!(
            "not ({})",
            self.blocked
                .iter()
                .map(|id| format!(
                    "(binary_id(={}) and test(={}))",
                    escape(&id.binary),
                    escape(&id.test)
                ))
                .collect::<Vec<_>>()
                .join(" or ")
        ))
    }
}

pub(super) fn admit(
    root: &Path,
    policy: &Policy,
    lane: Lane,
    required: &BTreeSet<TestId>,
    directory: &Path,
    artifacts: &Path,
    launcher: &super::launcher::Launcher,
) -> Result<Admission> {
    partition(
        &policy.prerequisites,
        lane,
        required,
        directory,
        artifacts,
        root,
        |capability| {
            bounded_probe(
                root,
                policy,
                directory,
                artifacts,
                capability,
                launcher.path(),
            )
        },
    )
}

fn partition(
    requirements: &[Requirement],
    lane: Lane,
    required: &BTreeSet<TestId>,
    directory: &Path,
    artifacts: &Path,
    root: &Path,
    mut probe: impl FnMut(Capability) -> ProbeOutcome,
) -> Result<Admission> {
    validate(requirements)?;
    let mut selected: BTreeMap<Capability, BTreeSet<TestId>> = BTreeMap::new();
    for requirement in requirements
        .iter()
        .filter(|item| item.lanes.contains(&lane))
    {
        let id = requirement.id();
        if !required.contains(&id) {
            return Err(error(format!(
                "stale prerequisite declaration in {} inventory: {id:?}",
                lane.name()
            )));
        }
        for capability in &requirement.capabilities {
            selected.entry(*capability).or_default().insert(id.clone());
        }
    }
    let mut admission = Admission {
        required: required.clone(),
        runnable: required.clone(),
        blocked: BTreeSet::new(),
        probes: vec![],
    };
    for (capability, affected) in selected {
        // Until a probe has completed, all its consumers remain incomplete in
        // the persisted record, including interruption during the probe itself.
        admission.blocked.extend(affected.iter().cloned());
        admission.runnable = required.difference(&admission.blocked).cloned().collect();
        admission.probes.push(ProbeRecord {
            capability,
            operation: capability.operation(),
            scope: capability.scope(root, artifacts),
            affected,
            duration_seconds: 0.0,
            completed: false,
            failure: None,
            infrastructure_failure: None,
        });
        admission.save(directory)?;
        let started = Instant::now();
        let record = admission.probes.last_mut().unwrap();
        match probe(capability) {
            ProbeOutcome::Completed(failure) => {
                record.completed = true;
                record.failure = failure;
            }
            ProbeOutcome::Incomplete(failure) => record.infrastructure_failure = Some(failure),
        }
        record.duration_seconds = started.elapsed().as_secs_f64();
        if let Some(failure) = &record.infrastructure_failure {
            eprintln!("validation: probe-{} incomplete: operation={} scope={} phase={} executable={} os_code={:?}: {}; capability unknown", capability.name(), record.operation, record.scope, failure.phase, failure.executable, failure.error.os_code, failure.error.message);
        } else if let Some(failure) = &record.failure {
            eprintln!("validation: probe-{} completed, capability unavailable: operation={} scope={} os_code={:?}: {}", capability.name(), record.operation, record.scope, failure.os_code, failure.message);
        }
        admission.blocked = admission
            .probes
            .iter()
            .filter(|record| !record.completed || record.failure.is_some())
            .flat_map(|record| record.affected.iter().cloned())
            .collect();
        admission.runnable = required.difference(&admission.blocked).cloned().collect();
        admission.save(directory)?;
    }
    debug_assert!(admission.runnable.is_disjoint(&admission.blocked));
    debug_assert_eq!(
        admission
            .runnable
            .union(&admission.blocked)
            .cloned()
            .collect::<BTreeSet<_>>(),
        *required
    );
    Ok(admission)
}

fn bounded_probe(
    root: &Path,
    policy: &Policy,
    directory: &Path,
    artifacts: &Path,
    capability: Capability,
    executable: &Path,
) -> ProbeOutcome {
    let mut phase = "before launch";
    let mut run = || -> Result<Option<ProbeFailure>> {
        if process::was_interrupted() {
            return Err(error("prerequisite probe interrupted"));
        }
        let label = format!("probe-{}", capability.name());
        let mut command = process::command(root, policy, executable);
        command
            .arg("__test-prerequisite")
            .arg(capability.name())
            .arg(artifacts);
        phase = "execute";
        let status = process::execute(&mut command, directory, &label, Duration::from_secs(5))?;
        phase = "exit status";
        if !status.success() {
            return Err(error(format!("probe process did not complete: {status}")));
        }
        phase = "read result";
        let bytes = fs::read(directory.join(format!("{label}.stdout.log")))?;
        phase = "decode result";
        Ok(serde_json::from_slice(&bytes)?)
    };
    match run() {
        Ok(failure) => ProbeOutcome::Completed(failure),
        Err(error) => {
            let process = error.downcast_ref::<process::ProcessFailure>();
            ProbeOutcome::Incomplete(InfrastructureFailure {
                phase: process.map_or_else(|| phase.into(), |error| error.phase.clone()),
                executable: executable.display().to_string(),
                error: ProbeFailure {
                    kind: "probe-incomplete".into(),
                    os_code: process
                        .and_then(|error| error.os_code)
                        .or_else(|| process::os_error_code(error.as_ref())),
                    message: error.to_string(),
                },
            })
        }
    }
}

/// Private subprocess entry: the existing process owner bounds even blocking
/// filesystem operations and kills/reaps the complete owned process group.
pub(crate) fn child(name: &str, artifacts: &Path) -> Result<()> {
    let capability: Capability = serde_json::from_value(serde_json::Value::String(name.into()))?;
    let failure = perform(capability, artifacts).err().map(ProbeFailure::from);
    println!("{}", serde_json::to_string(&failure)?);
    Ok(())
}

fn perform(capability: Capability, artifacts: &Path) -> io::Result<()> {
    use Capability::*;
    match capability {
        Ipv4LoopbackBind | Ipv4LoopbackConnect | Ipv6LoopbackBind | Ipv6LoopbackConnect => {
            let ip = if matches!(capability, Ipv6LoopbackBind | Ipv6LoopbackConnect) {
                IpAddr::V6(Ipv6Addr::LOCALHOST)
            } else {
                IpAddr::V4(Ipv4Addr::LOCALHOST)
            };
            let listener = TcpListener::bind(SocketAddr::new(ip, 0))?;
            if matches!(capability, Ipv4LoopbackConnect | Ipv6LoopbackConnect) {
                let mut client =
                    TcpStream::connect_timeout(&listener.local_addr()?, Duration::from_secs(1))?;
                client.set_write_timeout(Some(Duration::from_secs(1)))?;
                client.write_all(b"x")?;
                let (mut accepted, _) = listener.accept()?;
                accepted.set_read_timeout(Some(Duration::from_secs(1)))?;
                let mut byte = [0];
                accepted.read_exact(&mut byte)?;
                if byte != *b"x" {
                    return Err(io::Error::other("loopback data mismatch"));
                }
            }
            Ok(())
        }
        OwnedChild => {
            let mut child = OwnedChildGuard(Command::new("true").stdin(Stdio::null()).spawn()?);
            if !child.0.wait()?.success() {
                return Err(io::Error::other("owned child failed"));
            }
            Ok(())
        }
        OwnedChildSignals => {
            use std::os::unix::process::ExitStatusExt;
            for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGKILL] {
                let mut child = OwnedChildGuard(
                    Command::new("sleep")
                        .arg("30")
                        .stdin(Stdio::null())
                        .spawn()?,
                );
                // No PID discovery: this handle owns precisely the signalled child.
                if unsafe { libc::kill(child.0.id() as i32, 0) } != 0
                    || unsafe { libc::kill(child.0.id() as i32, signal) } != 0
                {
                    return Err(io::Error::last_os_error());
                }
                if child.0.wait()?.signal() != Some(signal) {
                    return Err(io::Error::other(
                        "owned child did not exit from the requested signal",
                    ));
                }
            }
            Ok(())
        }
        _ => {
            let directory = if capability == TargetFiles {
                tempfile::tempdir_in("target")?
            } else if matches!(capability, ArtifactFiles | ArtifactHardLinks) {
                tempfile::tempdir_in(artifacts)?
            } else {
                tempfile::tempdir()?
            };
            let original = directory.path().join("owned");
            let other = directory.path().join("other");
            let mut file = fs::File::create(&original)?;
            file.write_all(b"prerequisite")?;
            file.sync_all()?;
            drop(file);
            match capability {
                TempFiles | ArtifactFiles | TargetFiles => fs::rename(&original, &other)?,
                TempHardLinks | ArtifactHardLinks => fs::hard_link(&original, &other)?,
                TempModes => {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
                    fs::set_permissions(&original, fs::Permissions::from_mode(0o600))?;
                    if fs::metadata(&original)?.permissions().mode() & 0o777 != 0o600 {
                        return Err(io::Error::other("filesystem did not retain mode 0600"));
                    }
                    if fs::metadata(directory.path())?.permissions().mode() & 0o777 != 0o700 {
                        return Err(io::Error::other(
                            "filesystem did not retain directory mode 0700",
                        ));
                    }
                    return directory.close();
                }
                TempSymlinks => std::os::unix::fs::symlink(&original, &other)?,
                _ => unreachable!(),
            }
            if fs::read(&other)? != b"prerequisite" {
                return Err(io::Error::other("filesystem readback mismatch"));
            }
            directory.close()
        }
    }
}

struct OwnedChildGuard(Child);
impl Drop for OwnedChildGuard {
    fn drop(&mut self) {
        // try_wait reaps a completed child; never signal a potentially reused PID.
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(test: &str) -> TestId {
        TestId {
            binary: "fixture".into(),
            test: test.into(),
        }
    }
    fn requirement() -> Requirement {
        Requirement {
            binary: "fixture".into(),
            test: "socket".into(),
            lanes: vec![Lane::Default],
            capabilities: vec![
                Capability::Ipv4LoopbackBind,
                Capability::Ipv4LoopbackConnect,
            ],
            rationale: "HTTP wire assertion".into(),
        }
    }

    #[test]
    fn denied_operation_partitions_cases_and_preserves_executed_failure() {
        let dir = tempfile::tempdir().unwrap();
        let required = BTreeSet::from([id("socket"), id("memory")]);
        let admission = partition(
            &[requirement()],
            Lane::Default,
            &required,
            dir.path(),
            dir.path(),
            dir.path(),
            |capability| {
                if capability == Capability::Ipv4LoopbackConnect {
                    ProbeOutcome::Completed(Some(io::Error::from_raw_os_error(libc::EACCES).into()))
                } else {
                    ProbeOutcome::Completed(None)
                }
            },
        )
        .unwrap();
        assert_eq!(admission.runnable, BTreeSet::from([id("memory")]));
        assert_eq!(admission.blocked, BTreeSet::from([id("socket")]));
        assert_eq!(admission.required, required);
        let error = admission
            .finish(
                dir.path(),
                Err(super::super::failed("executed assertion failed")),
            )
            .unwrap_err();
        assert!(
            !error.is::<super::super::CheckFailed>(),
            "blocked required coverage stays incomplete"
        );
        let coverage: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("coverage.json")).unwrap()).unwrap();
        assert_eq!(coverage["execution"], "failed");
        assert_eq!(coverage["blocked"][0]["test"], "socket");
        assert_eq!(
            admission.probes[1].failure.as_ref().unwrap().os_code,
            Some(libc::EACCES)
        );
    }

    #[test]
    fn available_prerequisites_never_erase_execution_failure_or_missing_results() {
        let dir = tempfile::tempdir().unwrap();
        for execution in [
            Err(super::super::failed("later bind failed")),
            Err(error("missing JUnit")),
        ] {
            let admission = partition(
                &[requirement()],
                Lane::Default,
                &BTreeSet::from([id("socket")]),
                dir.path(),
                dir.path(),
                dir.path(),
                |_| ProbeOutcome::Completed(None),
            )
            .unwrap();
            assert!(admission.blocked.is_empty());
            let was_failed = execution
                .as_ref()
                .unwrap_err()
                .is::<super::super::CheckFailed>();
            let failure = admission.finish(dir.path(), execution).unwrap_err();
            assert_eq!(failure.is::<super::super::CheckFailed>(), was_failed);
        }
    }

    #[test]
    fn wholly_blocked_scope_and_invalid_declarations_cannot_pass() {
        let dir = tempfile::tempdir().unwrap();
        let required = BTreeSet::from([id("socket")]);
        let admission = partition(
            &[requirement()],
            Lane::Default,
            &required,
            dir.path(),
            dir.path(),
            dir.path(),
            |_| ProbeOutcome::Completed(Some(io::Error::from_raw_os_error(libc::EPERM).into())),
        )
        .unwrap();
        assert!(admission.runnable.is_empty());
        assert!(admission.finish(dir.path(), Ok(())).is_err());
        assert!(validate(&[requirement(), requirement()]).is_err());
        assert!(partition(
            &[requirement()],
            Lane::Default,
            &BTreeSet::from([id("renamed")]),
            dir.path(),
            dir.path(),
            dir.path(),
            |_| panic!("invalid inventory must precede probes")
        )
        .is_err());
        assert!(serde_json::from_str::<Capability>("\"unknown\"").is_err());
        let mut invalid = requirement();
        invalid.rationale.clear();
        assert!(validate(&[invalid]).is_err());
    }

    #[test]
    fn concurrent_filesystem_probes_own_distinct_scratch_resources() {
        let dir = tempfile::tempdir().unwrap();
        std::thread::scope(|scope| {
            let a = scope.spawn(|| perform(Capability::ArtifactFiles, dir.path()).unwrap());
            let b = scope.spawn(|| perform(Capability::ArtifactHardLinks, dir.path()).unwrap());
            a.join().unwrap();
            b.join().unwrap();
        });
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn real_probe_execution_failures_remain_unknown_and_preserve_independent_work() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let policy = Policy::read(root).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let not_executable = directory.path().join("not-executable");
        fs::write(&not_executable, "not an executable").unwrap();
        for (executable, phase, code) in [
            (
                directory.path().join("missing-xtask"),
                "spawn",
                Some(libc::ENOENT),
            ),
            (Path::new("/usr/bin/false").to_owned(), "exit status", None),
            (Path::new("/usr/bin/true").to_owned(), "decode result", None),
            (Path::new("/bin/echo").to_owned(), "decode result", None),
            (not_executable, "spawn", Some(libc::EACCES)),
        ] {
            let admission = partition(
                &[requirement()],
                Lane::Default,
                &BTreeSet::from([id("socket"), id("memory")]),
                directory.path(),
                directory.path(),
                root,
                |capability| {
                    bounded_probe(
                        root,
                        &policy,
                        directory.path(),
                        directory.path(),
                        capability,
                        &executable,
                    )
                },
            )
            .unwrap();
            assert_eq!(admission.runnable, BTreeSet::from([id("memory")]));
            if let Some(code) = code {
                let failure: serde_json::Value = serde_json::from_slice(
                    &fs::read(directory.path().join("probe-ipv4-loopback-bind.error.json"))
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(failure["phase"], "spawn");
                assert_eq!(failure["executable"], executable.display().to_string());
                assert_eq!(failure["os_code"], code);
            }
            for record in &admission.probes {
                assert!(!record.completed);
                assert!(
                    record.failure.is_none(),
                    "a capability denial requires a completed probe"
                );
                let failure = record.infrastructure_failure.as_ref().unwrap();
                assert_eq!(failure.phase, phase);
                assert_eq!(failure.executable, executable.display().to_string());
                assert_eq!(failure.error.os_code, code);
            }
            let failure = admission
                .finish(
                    directory.path(),
                    Err(super::super::failed("independent assertion")),
                )
                .unwrap_err();
            assert!(failure.to_string().contains("capability unknown"));
            assert!(failure.to_string().contains("independent assertion"));
        }
    }
}
