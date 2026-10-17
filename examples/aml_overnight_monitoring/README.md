# Overnight AML transaction monitoring

Score every transaction of one day through a risk engine whose rules the flow never
reads, open a case for each flagged transaction, and resume a killed run without
scoring a transaction or opening a case twice.

```text
account_history --+
                  +--> profile --> score --TransactionCleared--> cleared
transactions -----+                  |
                                     `--AlertRaised--> open_case --CaseOpened--> case_register
```

`profile` pairs each transaction with its paying account's history. `score` sends the
pair to the simulated risk engine and records its verdict, cleared or flagged, with the
rules version it came from and, for a flag, every rule that fired. `open_case` sends
each alert to the simulated case system. Both systems run in process behind the named
effect ports `risk_engine` and `case_system`, and neither is built until its first live
call, so a strict replay never builds them. The engine's rules live in
[risk_engine.rs](risk_engine.rs). They are illustrative, not an AML model, and the
dataset's laundering label never reaches the engine. The example makes no compliance
claim.

## Run the fixture

Run from the repository root. The checked-in fixture is a synthetic day of 240
transactions, planted so every rule fires, one transaction trips two rules, and some
accounts have no history. It needs no download and no extra Cargo feature:

```sh
rm -f target/aml-risk-calls.jsonl target/aml-case-calls.jsonl
cargo run -p obzenflow --example aml_overnight_monitoring
```

Cleared transactions go to `target/aml-cleared.csv` and cases to `target/aml-cases.csv`.
Each simulated system appends every call it receives to its own ledger,
`target/aml-risk-calls.jsonl` or `target/aml-case-calls.jsonl`, and answers a repeated
key with its first answer, logged as `replayed`. A first call's line holds the
system's answer:

```json
{"key":"aml-score:2800117","result":"scored","flow_id":"flow_…","verdict":"flagged","reasons":["format_switch","thin_history_large_transfer"],"rules_version":"sim-2026.10"}
{"key":"aml-case:2800117","result":"opened","flow_id":"flow_…","case_number":6}
```

A ledger holds the calls of one fresh run and the resumes that continue it, so remove
both ledgers before each fresh run and never before a resume. Replay a run without
calling either system, using the archive path the run prints:

```sh
cargo run -p obzenflow --example aml_overnight_monitoring -- \
  --replay-from <archive> --verify
```

[ledger-check.jq](ledger-check.jq) judges both ledgers against the runs that wrote them.
Give it every run's `run_manifest.json`, in any order, then the ledgers:

```sh
jq -nr -f examples/aml_overnight_monitoring/ledger-check.jq \
  <archive>/run_manifest.json target/aml-case-calls.jsonl target/aml-risk-calls.jsonl
```

It refuses a call from a run outside the lineage, a ledger with no call from the run
that started it, a key decided twice, and more repeats than the lineage has resumes.

## AMLworld at scale

[AMLworld](https://arxiv.org/abs/2306.16424) is IBM's synthetic transaction dataset.
Download `HI-Small_Trans.csv` from
[IBM's Kaggle listing](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml),
which needs a free Kaggle account, into `data/`. IBM publishes it under the Community
Data License Agreement, Sharing, version 1.0. Then prepare one day once, before the first
run. The command writes the day's transactions, each numbered by its row in the original
file, and one history row for each account that paid on an earlier day. It needs a POSIX
`awk`, so on Windows run it under WSL:

```sh
awk -F, -v day=2022/09/05 -v txns=data/aml-2022-09-05.csv -v history=data/aml-history-2022-09-05.csv '
  BEGIN {
    OFS = ","
    n = split("ACH,Bitcoin,Cash,Cheque,Credit Card,Reinvestment,Wire", format, ",")
    print "txn_id,timestamp,from_bank,from_account,to_bank,to_account,amount_received,receiving_currency,amount_paid,payment_currency,payment_format,is_laundering" > txns
    print "bank,account,payments,counterparties,formats" > history
  }
  NR == 1 { next }
  substr($1, 1, 10) == day { print NR - 1, $0 > txns; next }
  substr($1, 1, 10) < day {
    account = $2 "," $3
    payments[account]++
    if (!((account, $4 "/" $5) in paid)) { paid[account, $4 "/" $5]; counterparties[account]++ }
    used[account, $10]
  }
  END {
    for (account in payments) {
      formats = ""
      for (i = 1; i <= n; i++) if ((account, format[i]) in used) formats = formats (formats == "" ? "" : ";") format[i]
      print account, payments[account], counterparties[account], formats > history
    }
  }' data/HI-Small_Trans.csv
```

A resume checks that its input files are the ones the killed run read, by inode, size
and timestamps, so running the prep again makes a new input even when its output is
identical.

The walkthrough uses two terminals and the `obzenflow` CLI. First an uninterrupted
control run. Once it completes, move its ledgers aside as its evidence and check them:

```sh
rm -f target/aml-risk-calls.jsonl target/aml-case-calls.jsonl
cargo run --release -p obzenflow --features web-host --example aml_overnight_monitoring -- \
  --config examples/aml_overnight_monitoring/amlworld.toml \
  --server --startup-mode manual --server-port 9090
obzenflow start --server http://127.0.0.1:9090 --follow
mkdir -p target/aml-control
mv target/aml-risk-calls.jsonl target/aml-case-calls.jsonl target/aml-control/
jq -nr -f examples/aml_overnight_monitoring/ledger-check.jq \
  <control-run-dir>/run_manifest.json \
  target/aml-control/aml-case-calls.jsonl target/aml-control/aml-risk-calls.jsonl
```

Then start the run to kill with the same two commands. When `show` reports about half
the transactions scored, SIGKILL the first terminal's process and inspect the archive. It
holds the verdicts and cases committed so far, and a transactions source that has not
reached the end of its file:

```sh
kill -9 <pid>
obzenflow show <killed-run-dir>
```

Resume and follow, then check both ledgers against the killed and resumed runs:

```sh
cargo run --release -p obzenflow --features web-host --example aml_overnight_monitoring -- \
  --config examples/aml_overnight_monitoring/amlworld.toml \
  --server --startup-mode manual --server-port 9090 \
  --resume-from <killed-run-dir>
obzenflow start --server http://127.0.0.1:9090 --follow
jq -nr -f examples/aml_overnight_monitoring/ledger-check.jq \
  <killed-run-dir>/run_manifest.json <resumed-run-dir>/run_manifest.json \
  target/aml-case-calls.jsonl target/aml-risk-calls.jsonl
```

Each repeat count is 0 or 1, because a kill can cut off at most one call per system and
the resume makes it again with the same key. The resumed history ends with the same verdict
for every transaction and the same cases as the control. In the resumed run, `show`
marks the verdicts rebuilt from the killed run's journal and leaves the rest unmarked.
Finally, replay the resumed run and verify it, after which neither ledger has grown:

```sh
cargo run --release -p obzenflow --example aml_overnight_monitoring -- \
  --config examples/aml_overnight_monitoring/amlworld.toml --replay-from <resumed-run-dir>
obzenflow verify --baseline <resumed-run-dir> --candidate <replay-run-dir>
```

Source: [flow](flow.rs), [stages](stages.rs), [risk engine](risk_engine.rs) and
[case system](case_system.rs).
See the [examples index](../README.md) for published tutorials.
