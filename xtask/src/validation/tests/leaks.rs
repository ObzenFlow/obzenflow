// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Exercise the actual captured-output/JUnit boundary without orphaning a child.
//! The controller temporarily owns copies of the fixture's output descriptors.
//! Nextest still lists and executes the existing Cargo-built Rust test binary.

use super::*;
use std::{
    io,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::{net::UnixDatagram, process::CommandExt},
    },
    sync::mpsc,
};

const CHANNEL: &str = "OBZENFLOW_LEAK_CONTROL_FD";
const MODE: &str = "OBZENFLOW_LEAK_CONTROL_MODE";
const OUTPUT: &str = "validation::tests::leaks::output_fixture";
const ASSERTION: &str = "validation::tests::leaks::assertion_fixture";
const PASSING: &str = "validation::tests::leaks::passing_fixture";
const BINARY: &str = "xtask::bin/xtask";

#[test]
#[ignore = "owned by real_nextest_leaks_fail_acceptance_with_identity"]
fn output_fixture() {
    let fd: i32 = std::env::var(CHANNEL).unwrap().parse().unwrap();
    // The controller explicitly inherits this descriptor into the nested runner.
    let socket = unsafe { UnixDatagram::from_raw_fd(fd) };
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    send_outputs(&socket).unwrap();
    let mut acknowledgement = [0];
    assert_eq!(socket.recv(&mut acknowledgement).unwrap(), 1);
    assert_eq!(acknowledgement, [1]);
    eprintln!("output fixture: controller acknowledged descriptor ownership");
    if std::env::var(MODE).unwrap() == "interrupted" {
        // The controller interrupts Nextest after receiving the descriptors.
        // Its normal signal owner terminates this process; no sleep is evidence.
        loop {
            std::thread::park();
        }
    }
}

#[test]
#[ignore = "owned by real_nextest_leaks_fail_acceptance_with_identity"]
fn assertion_fixture() {
    panic!("independent assertion failure retained beside the leak");
}

#[test]
#[ignore = "owned by real_nextest_leaks_fail_acceptance_with_identity"]
fn passing_fixture() {
    assert_eq!(std::env::var(MODE).unwrap(), "mixed");
    eprintln!("independent work completed");
}

fn send_outputs(socket: &UnixDatagram) -> io::Result<()> {
    // Two headers provide aligned storage larger than CMSG_SPACE(two fds) on
    // macOS and Linux. Only the initialised control-message length is sent.
    unsafe {
        let mut storage: [libc::cmsghdr; 2] = std::mem::zeroed();
        let mut parent = libc::getppid();
        let mut data = libc::iovec {
            iov_base: (&mut parent as *mut libc::pid_t).cast(),
            iov_len: std::mem::size_of_val(&parent),
        };
        let mut message: libc::msghdr = std::mem::zeroed();
        message.msg_iov = &mut data;
        message.msg_iovlen = 1;
        message.msg_control = storage.as_mut_ptr().cast();
        message.msg_controllen = libc::CMSG_SPACE(8) as _;
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(8) as _;
        libc::CMSG_DATA(header)
            .cast::<[i32; 2]>()
            .write_unaligned([1, 2]);
        if libc::sendmsg(socket.as_raw_fd(), &message, 0) != data.iov_len as isize {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn receive_outputs(socket: &UnixDatagram) -> io::Result<(libc::pid_t, [OwnedFd; 2])> {
    unsafe {
        let mut storage: [libc::cmsghdr; 2] = std::mem::zeroed();
        let mut parent: libc::pid_t = 0;
        let mut data = libc::iovec {
            iov_base: (&mut parent as *mut libc::pid_t).cast(),
            iov_len: std::mem::size_of_val(&parent),
        };
        let mut message: libc::msghdr = std::mem::zeroed();
        message.msg_iov = &mut data;
        message.msg_iovlen = 1;
        message.msg_control = storage.as_mut_ptr().cast();
        message.msg_controllen = std::mem::size_of_val(&storage) as _;
        if libc::recvmsg(socket.as_raw_fd(), &mut message, 0) < 0 {
            return Err(io::Error::last_os_error());
        }
        let header = libc::CMSG_FIRSTHDR(&message);
        assert!(!header.is_null());
        assert_eq!((*header).cmsg_level, libc::SOL_SOCKET);
        assert_eq!((*header).cmsg_type, libc::SCM_RIGHTS);
        assert_eq!((*header).cmsg_len as usize, libc::CMSG_LEN(8) as usize);
        let descriptors = libc::CMSG_DATA(header).cast::<[i32; 2]>().read_unaligned();
        let owned = descriptors.map(|fd| OwnedFd::from_raw_fd(fd));
        for fd in &owned {
            if libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        assert!(parent > 1);
        Ok((parent, owned))
    }
}

fn id(name: &str) -> plan::TestId {
    plan::TestId {
        binary: BINARY.into(),
        test: name.into(),
    }
}

fn reuse_current_binary(root: &Path, policy: &Policy, directory: &Path) {
    // Reuse Nextest's documented binary metadata interface. Never ask Cargo to
    // rebuild a binary while its own tests are running, and never invent results.
    let metadata = process::capture(
        root,
        policy,
        "cargo",
        &["metadata", "--locked", "--offline", "--format-version", "1"],
        directory,
        "cargo-metadata",
    )
    .unwrap();
    let metadata: Value = serde_json::from_str(&metadata).unwrap();
    let package = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "xtask")
        .unwrap();
    let rust =
        process::capture(root, policy, "rustc", &["-vV"], directory, "rust-version").unwrap();
    let host = rust
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap();
    let libdir = process::capture(
        root,
        policy,
        "rustc",
        &["--print", "target-libdir"],
        directory,
        "rust-libdir",
    )
    .unwrap();
    let binaries = json!({
        "rust-build-meta": {
            "target-directory": metadata["target_directory"],
            "base-output-directories": ["debug"], "non-test-binaries": {},
            "build-script-out-dirs": {}, "linked-paths": [],
            "platforms": {"host": {"platform": {"triple": host, "target-features": "unknown"}, "libdir": {"status": "available", "path": libdir.trim()}}, "targets": []}
        },
        "rust-binaries": {BINARY: {
            "binary-id": BINARY, "binary-name": "xtask", "package-id": package["id"],
            "kind": "bin", "binary-path": std::env::current_exe().unwrap(), "build-platform": "target"
        }}
    });
    fs::write(
        directory.join("cargo.json"),
        serde_json::to_vec(&metadata).unwrap(),
    )
    .unwrap();
    fs::write(
        directory.join("binaries.json"),
        serde_json::to_vec(&binaries).unwrap(),
    )
    .unwrap();
}

#[test]
fn real_nextest_leaks_fail_acceptance_with_identity() {
    const CONTROLLER: &str = "OBZENFLOW_LEAK_CONTROLLER";
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let policy = Policy::read(root).unwrap();
    if let Some(directory) = std::env::var_os(CONTROLLER) {
        run_controls(root, &policy, Path::new(&directory));
        return;
    }
    let directory = root
        .join("target/test-runs")
        .join(format!("leak-controls-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&directory).unwrap();
    eprintln!("real leak controls: {}", directory.display());
    fs::write(
        directory.join("source.json"),
        serde_json::to_vec_pretty(&source::identity(root).unwrap()).unwrap(),
    )
    .unwrap();
    // Cargo's libtest entry can run sibling tests on other threads. Keep fd
    // reception in a dedicated process: on macOS recvmsg cannot set CLOEXEC
    // atomically, so a concurrent sibling spawn could otherwise inherit it.
    let image = launcher::Launcher::retain(&directory).unwrap();
    let status = process::execute(
        process::command(root, &policy, image.path())
            .args([
                "--exact",
                "validation::tests::leaks::real_nextest_leaks_fail_acceptance_with_identity",
                "--nocapture",
            ])
            .env(CONTROLLER, &directory),
        &directory,
        "controller",
        Duration::from_secs(30),
    )
    .unwrap();
    assert!(
        status.success(),
        "leak controls failed; artifacts={}",
        directory.display()
    );
}

fn run_controls(root: &Path, policy: &Policy, directory: &Path) {
    reuse_current_binary(root, policy, directory);

    for mode in ["legacy", "strict", "released", "mixed", "interrupted"] {
        let output = directory.join(mode);
        fs::create_dir(&output).unwrap();
        let mut config: toml::Value =
            toml::from_str(include_str!("../../../../.config/nextest.toml")).unwrap();
        if mode == "legacy" {
            config["profile"]["default"]
                .as_table_mut()
                .unwrap()
                .insert("leak-timeout".into(), toml::Value::String("200ms".into()));
            assert!(plan::validate_leak_policy(&config).is_err());
        } else {
            plan::validate_leak_policy(&config).unwrap();
        }
        config["profile"]["ci-fast"]["junit"]["path"] =
            toml::Value::String(output.join("junit.xml").to_string_lossy().into_owned());
        let config_path = output.join("nextest.toml");
        fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
        let expected = if mode == "mixed" {
            BTreeSet::from([id(OUTPUT), id(ASSERTION), id(PASSING)])
        } else {
            BTreeSet::from([id(OUTPUT)])
        };
        let filter = expected
            .iter()
            .map(|id| format!("test(={})", id.test))
            .collect::<Vec<_>>()
            .join(" | ");
        let (receiver, sender) = UnixDatagram::pair().unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let fd = sender.as_raw_fd();
        let mut command = process::command(root, policy, "cargo");
        command
            .args([
                "nextest",
                "run",
                "--profile",
                "ci-fast",
                "--user-config-file",
                "none",
                "--run-ignored",
                "only",
                "-E",
                &filter,
            ])
            .arg("--config-file")
            .arg(config_path)
            .arg("--cargo-metadata")
            .arg(directory.join("cargo.json"))
            .arg("--binaries-metadata")
            .arg(directory.join("binaries.json"))
            .args(nextest_execution_args(policy))
            .env(CHANNEL, fd.to_string())
            .env(MODE, mode);
        // Only the child receives an inheritable channel. The controller keeps
        // its normal close-on-exec descriptors; concurrent spawns cannot copy it.
        unsafe {
            command.pre_exec(move || {
                if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let (release, held) = mpsc::channel::<()>();
        let status = std::thread::scope(|scope| {
            let controller = scope.spawn(move || {
                let (runner, descriptors) = receive_outputs(&receiver).unwrap();
                let descriptors = if mode == "released" {
                    drop(descriptors);
                    None
                } else {
                    Some(descriptors)
                };
                receiver.send(&[1]).unwrap();
                if mode == "interrupted" {
                    assert_eq!(unsafe { libc::kill(runner, libc::SIGTERM) }, 0);
                }
                // Exit/panic of the process owner drops `release`, too. A timeout
                // is a fixture failure, never the evidence that a leak occurred.
                let released = held.recv_timeout(Duration::from_secs(30));
                drop(descriptors);
                assert!(!matches!(released, Err(mpsc::RecvTimeoutError::Timeout)));
            });
            let status = process::execute_nextest(&mut command, &output, Duration::from_secs(20));
            drop(release);
            controller.join().unwrap();
            status.unwrap()
        });
        let acceptance = evaluate_nextest(&output, &expected, status.success());
        if mode == "interrupted" {
            assert!(!status.success());
            assert!(acceptance.is_err());
            assert!(fs::read_to_string(output.join("tests.stderr.log"))
                .unwrap()
                .contains("Cancelling due to signal"));
            assert!(
                evaluate_nextest(&output, &expected, true).is_err(),
                "interrupted evidence cannot pass with a zero exit"
            );
            continue;
        }
        let xml = fs::read_to_string(output.join("junit.xml")).unwrap();
        let results = report::inspect(&xml).unwrap();
        assert_eq!(
            results.observed, expected,
            "independent work must finish: {mode}"
        );
        assert_eq!(results.attempts, expected.len(), "zero retries: {mode}");
        if matches!(mode, "legacy" | "released") {
            assert!(status.success(), "{mode}");
            acceptance.unwrap();
            assert!(results.failed.is_empty());
            let stderr = fs::read_to_string(output.join("tests.stderr.log")).unwrap();
            if mode == "legacy" {
                assert!(stderr.contains("LEAK") && stderr.contains(OUTPUT));
            } else {
                assert!(!stderr.contains("LEAK"));
            }
        } else {
            assert!(
                !status.success(),
                "a detected leak must fail the runner: {mode}"
            );
            assert!(acceptance.is_err());
            assert!(evaluate_nextest(&output, &expected, true).is_err());
            let failures = if mode == "mixed" {
                BTreeSet::from([id(OUTPUT), id(ASSERTION)])
            } else {
                BTreeSet::from([id(OUTPUT)])
            };
            assert_eq!(results.failed, failures);
            let stderr = fs::read_to_string(output.join("tests.stderr.log")).unwrap();
            assert!(stderr.contains("LEAK-FAIL") && stderr.contains(OUTPUT));
            assert!(xml.contains("controller acknowledged descriptor ownership"));
        }
    }
}
