// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[cfg(feature = "prometheus")]
#[tokio::test]
async fn reporting_without_a_host_fails_before_building_or_opening_replay() {
    use obzenflow_dsl::FlowDefinition;
    use obzenflow_infra::application::{ApplicationError, FlowApplication};

    let dir = tempfile::tempdir_in("target").unwrap();
    let config = dir.path().join("obzenflow.toml");
    for startup in ["auto", "manual"] {
        std::fs::write(
            &config,
            format!(
                "[server]\nenabled = false\nstartup_mode = \"{startup}\"\n[metrics]\nenabled = true\n"
            ),
        )
        .unwrap();
        for replay in [false, true] {
            let mut args = vec!["reporting-admission".into()];
            if replay {
                args.extend([
                    "--replay-from".into(),
                    dir.path().join("absent-archive").into_os_string(),
                ]);
            }
            let result = FlowApplication::builder()
                .with_config_file(&config)
                .with_cli_args(args)
                .run_async(FlowDefinition::materialize(|_| {
                    panic!("invalid reporting configuration must not materialise a flow")
                }))
                .await;
            let Err(ApplicationError::InvalidConfiguration(message)) = result else {
                panic!("reporting admission must precede archive opening: {result:?}");
            };
            assert_eq!(
                message,
                "config error at server.enabled: enabled Prometheus reporting requires true and the web-host capability"
            );
        }
    }
}
