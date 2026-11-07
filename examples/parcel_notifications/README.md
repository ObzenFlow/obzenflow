# Parcel notifications

Turn carrier updates into customer emails and texts, with middleware that the code sets, a config file refines and Studio shows.

Run from the repository root. The depots and the messaging provider are simulated, so no external service is needed.

```sh
cargo run -p obzenflow --example parcel_notifications
```

To replay a run without calling the simulated services, use the archive path the run printed:

```sh
cargo run -p obzenflow --example parcel_notifications -- \
  --replay-from <archive> --verify
```

## Inspect in Studio

With studiod running on 127.0.0.1:7010, run with the Studio config:

```sh
cargo run -p obzenflow --example parcel_notifications --features studio -- \
  --config examples/parcel_notifications/obzenflow.studio.toml
```

Open http://127.0.0.1:7010 and choose parcel_notifications. The run waits for Start, so you can inspect each stage's middleware first, and it stays up afterwards until you stop it with Ctrl-C. [`obzenflow.studio.toml`](obzenflow.studio.toml) changes some of the code's settings, and Studio shows which values came from the file. Replaying that run with the command above also lists the settings that differ from the archive.

Source: [flow](flow.rs), [handlers](handlers.rs) and [services](services.rs).
See the [examples index](../README.md) for published tutorials.
