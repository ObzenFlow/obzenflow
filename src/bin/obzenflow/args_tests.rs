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
        "cli_verify.tick",
    ])
    .unwrap();
    let Command::Inspect(args) = cli.command else {
        panic!("inspect must select archive inspection");
    };
    assert_eq!(args.run_dir, PathBuf::from("run"));
    assert_eq!(args.stage.as_deref(), Some("ticks"));
    assert_eq!(args.event_type.as_deref(), Some("cli_verify.tick"));

    for subcommand in ["inspect", "export-jsonl"] {
        let error = Cli::try_parse_from(["obzenflow", "journal", subcommand, "run"])
            .err()
            .expect("the journal namespace must not remain as a second route");
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }
}

#[test]
fn verify_keeps_its_run_comparison_and_judges_call_logs_as_a_subcommand() {
    let cli = Cli::try_parse_from(["obzenflow", "verify", "--baseline", "a", "--candidate", "b"])
        .unwrap();
    let Command::Verify(args) = cli.command else {
        panic!("verify must select verification");
    };
    assert!(args.check.is_none());
    assert_eq!(args.baseline, Some(PathBuf::from("a")));
    assert_eq!(args.candidate, Some(PathBuf::from("b")));

    let cli = Cli::try_parse_from([
        "obzenflow",
        "verify",
        "calls",
        "--run",
        "resumed",
        "--run",
        "killed",
        "target/aml-risk-calls.jsonl",
        "target/aml-case-calls.jsonl",
    ])
    .unwrap();
    let Command::Verify(args) = cli.command else {
        panic!("verify must select verification");
    };
    let Some(VerifyCheck::Calls(calls)) = args.check else {
        panic!("calls must select the lineage check");
    };
    assert_eq!(
        calls.runs,
        [PathBuf::from("resumed"), PathBuf::from("killed")]
    );
    assert_eq!(
        calls.call_logs,
        [
            PathBuf::from("target/aml-risk-calls.jsonl"),
            PathBuf::from("target/aml-case-calls.jsonl")
        ]
    );

    let error = Cli::try_parse_from(["obzenflow", "verify", "calls", "risk.jsonl"])
        .err()
        .expect("a lineage check needs its runs");
    assert_eq!(
        error.kind(),
        clap::error::ErrorKind::MissingRequiredArgument
    );
    assert!(error.to_string().contains("--run <RUN_DIR>"), "{error}");
    for incomplete in [
        vec!["obzenflow", "verify", "calls", "--run", "killed"],
        vec!["obzenflow", "verify", "--baseline", "a"],
        vec![
            "obzenflow",
            "verify",
            "--baseline",
            "a",
            "--candidate",
            "b",
            "calls",
            "--run",
            "killed",
            "risk.jsonl",
        ],
    ] {
        assert!(
            Cli::try_parse_from(&incomplete).is_err(),
            "{incomplete:?} is refused"
        );
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

#[test]
fn verify_long_help_lists_exit_codes() {
    use clap::CommandFactory;
    let mut command = Cli::command();
    let top = command.render_help().to_string();
    assert!(
        top.contains(
            "Verify a candidate run's recorded output against a baseline, or call logs against \
             their lineage\n"
        ),
        "{top}"
    );
    let verify = command
        .find_subcommand_mut("verify")
        .expect("verify subcommand");
    let long = verify.render_long_help().to_string();
    for text in [
        "For runs, exit codes are 0 certified match, 1 divergence, 2 certified region matched \
         with uncertified stages and 3 refused. For call logs, 0 every log passes, 1 a log \
         breaks a rule, 2 a log is uncertified and 3 refused. Any other failure exits 4, and so \
         does a wrong or missing argument.",
        "--baseline <PATH>",
        "The run directory the candidate is compared to",
        "--candidate <PATH>",
        "The run directory under test",
        "--report-path <PATH>",
        "Write the comparison report here",
        "--max-divergences <N>",
        "How many divergences to list",
        "[default: 5]",
    ] {
        assert!(long.contains(text), "missing {text:?} in {long}");
    }
}
