// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[test]
fn start_view_options_require_follow_and_match_show() {
    let base = ["obzenflow", "start", "--server", "http://127.0.0.1:9090"];
    assert!(
        Cli::try_parse_from(base).is_ok(),
        "bare start remains available"
    );
    for options in [
        vec!["--jsonl"],
        vec!["--include-runtime"],
        vec!["--full"],
        vec!["--explain"],
        vec!["--compact"],
        vec!["--color", "never"],
        vec!["--jsonl", "--include-runtime"],
        vec!["--compact", "--include-runtime"],
    ] {
        let error = Cli::try_parse_from(base.into_iter().chain(options.iter().copied()))
            .err()
            .expect("view options cannot be silently ignored by bare start");
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        assert!(error.to_string().contains("--follow"));
        assert!(
            Cli::try_parse_from(
                base.into_iter()
                    .chain(["--follow"])
                    .chain(options.iter().copied())
            )
            .is_ok(),
            "start --follow accepts {options:?}"
        );
        assert!(
            Cli::try_parse_from(["obzenflow", "show", "run"].into_iter().chain(options)).is_ok()
        );
    }
}

#[test]
fn show_has_no_execution_flag_and_old_view_spellings_are_rejected() {
    for flag in [
        "--start",
        "--run",
        "--verbose",
        "--quiet",
        "--detail",
        "--json",
    ] {
        let error = Cli::try_parse_from(["obzenflow", "show", "run", flag])
            .err()
            .expect("only the supported read-only vocabulary is accepted");
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }
}

#[test]
fn inspect_is_a_direct_verb_and_the_journal_command_group_is_removed() {
    let cli = Cli::try_parse_from([
        "obzenflow",
        "inspect",
        "run",
        "--stage",
        "ticks",
        "--event-type",
        "cli_verify.tick.v1",
    ])
    .unwrap();
    let Command::Inspect(args) = cli.command else {
        panic!("inspect must select archive inspection");
    };
    assert_eq!(args.run_dir, PathBuf::from("run"));
    assert_eq!(args.stage.as_deref(), Some("ticks"));
    assert_eq!(args.event_type.as_deref(), Some("cli_verify.tick.v1"));

    for subcommand in ["inspect", "export-jsonl"] {
        let error = Cli::try_parse_from(["obzenflow", "journal", subcommand, "run"])
            .err()
            .expect("the journal namespace must not remain as a second route");
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }
}

#[test]
fn control_origins_are_http_loopback_only() {
    for origin in [
        "http://localhost:9090",
        "http://LOCALHOST:9090/",
        "http://127.0.0.1:9090",
        "http://127.23.4.5:9090/",
        "http://[::1]:9090/",
    ] {
        assert!(
            validate_server_origin(&origin.parse().unwrap()).is_ok(),
            "{origin}"
        );
    }
    for origin in [
        "https://localhost:9090",
        "ftp://127.0.0.1:9090",
        "http://0.0.0.0:9090",
        "http://[::]:9090",
        "http://192.168.1.1:9090",
        "http://example.com:9090",
        "http://localhost.example.com:9090",
        "http://user:password@localhost:9090",
        "http://localhost:9090/api/flow",
        "http://localhost:9090/?query=value",
        "http://localhost:9090/#fragment",
    ] {
        assert!(
            validate_server_origin(&origin.parse().unwrap()).is_err(),
            "{origin}"
        );
    }
}
