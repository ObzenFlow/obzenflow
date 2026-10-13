// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

impl Renderer {
    pub(super) fn configuration_summary(
        &self,
        output: &mut impl Write,
        changes: &Result<Vec<RunConfigChanges>, JournalReadError>,
    ) -> Result<(), Error> {
        let lines = match changes {
            Ok(changes) => configuration_lines(changes),
            Err(error) => vec![format!("  Unreadable  {error}")],
        };
        if lines.is_empty() {
            return Ok(());
        }
        writeln!(output)?;
        self.summary_line(output, HEADING, "CONFIGURATION")?;
        for line in lines {
            self.summary_line(output, BODY, &line)?;
        }
        Ok(())
    }
}

fn configuration_lines(changes: &[RunConfigChanges]) -> Vec<String> {
    let mut lines = Vec::new();
    for compared in changes {
        let run = compared.run.display();
        match &compared.changes {
            ConfigChanges::Recorded(differences) if differences.is_empty() => {}
            ConfigChanges::Recorded(differences) => {
                lines.push(format!("  Changed since {run}"));
                let keys: Vec<String> = differences.iter().map(key_label).collect();
                let width = keys
                    .iter()
                    .map(|key| key.chars().count())
                    .max()
                    .unwrap_or(0);
                for (key, difference) in keys.iter().zip(differences) {
                    lines.push(format!(
                        "    {key:<width$}   {} -> {}",
                        value_text(difference.recorded.as_ref()),
                        value_text(difference.current.as_ref()),
                    ));
                }
            }
            ConfigChanges::NotRecorded => {
                lines.push(format!("  Configuration of {run} was not recorded"));
            }
            ConfigChanges::Unreadable(error) => {
                lines.push(format!(
                    "  Configuration of {run} could not be read: {error}"
                ));
            }
        }
    }
    lines
}

fn key_label(difference: &ConfigDifference) -> String {
    match &difference.point {
        Some(point) => format!("{} ({point})", difference.key),
        None => difference.key.clone(),
    }
}

fn value_text(value: Option<&serde_json::Value>) -> String {
    value.map_or_else(|| "(none)".to_string(), serde_json::Value::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn difference(
        key: &str,
        point: Option<&str>,
        old: Option<serde_json::Value>,
        new: serde_json::Value,
    ) -> ConfigDifference {
        ConfigDifference {
            key: key.into(),
            point: point.map(str::to_string),
            recorded: old,
            current: Some(new),
        }
    }

    #[test]
    fn each_changed_key_shows_its_recorded_old_and_new_values() {
        let changes = [
            RunConfigChanges {
                run: "/runs/r1".into(),
                changes: ConfigChanges::Recorded(vec![
                    difference(
                        "app.accounts_file",
                        None,
                        Some(json!(
                            "examples/http_ingestion_piggy_bank_demo/accounts.yaml"
                        )),
                        json!("deploy/staging/accounts.yaml"),
                    ),
                    difference(
                        "runtime.max_lineage_depth",
                        Some("stage:posted"),
                        None,
                        json!(5),
                    ),
                ]),
            },
            RunConfigChanges {
                run: "/runs/r0".into(),
                changes: ConfigChanges::Recorded(Vec::new()),
            },
            RunConfigChanges {
                run: "/runs/old".into(),
                changes: ConfigChanges::NotRecorded,
            },
        ];
        assert_eq!(
            configuration_lines(&changes),
            [
                "  Changed since /runs/r1",
                "    app.accounts_file                          \"examples/http_ingestion_piggy_bank_demo/accounts.yaml\" -> \"deploy/staging/accounts.yaml\"",
                "    runtime.max_lineage_depth (stage:posted)   (none) -> 5",
                "  Configuration of /runs/old was not recorded",
            ]
        );
        assert!(configuration_lines(&changes[1..2]).is_empty());
    }
}
