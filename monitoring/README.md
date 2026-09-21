# Prometheus and Grafana

This directory runs a development monitoring stack for an ObzenFlow application
exposing `/metrics` on port 9090. It includes Prometheus and Grafana dashboards
for flow and stage metrics.

## Start the stack

Docker and the `docker-compose` command are required by `setup.sh`.
From the repository root, start the metrics example:

```bash
cargo run -p obzenflow --example prometheus_demo --features prometheus,web-host -- \
  --config examples/prometheus_demo/obzenflow.prometheus.toml
```

The example waits for Play. In another terminal, start the monitoring services:

```bash
cd monitoring
./setup.sh
```

Once Prometheus and Grafana are ready, start processing:

```bash
curl -X POST http://localhost:9090/api/flow/control \
  -H 'Content-Type: application/json' -d '{"action":"play"}'
```

| Service | Address |
| --- | --- |
| ObzenFlow metrics | [localhost:9090/metrics](http://localhost:9090/metrics) |
| Prometheus | [localhost:9091](http://localhost:9091) |
| Grafana | [localhost:3000](http://localhost:3000) |

Grafana's initial login is `admin` / `admin`. Open **Dashboards → Browse** for
the Flow Overview and Stage Details dashboards. Prometheus scrapes every five
seconds and retains collected samples after the example finishes.

For an existing application, compile the facade's `prometheus` and `web-host`
features and enable both services in its configuration:

```toml
[server]
enabled = true

[metrics]
enabled = true
```

See [the metrics example guide](../examples/prometheus_demo/README.md) for
example behavior, disabled reporting, and replay. Scrape targets live in
[prometheus.yml](prometheus/prometheus.yml); dashboard provisioning lives in
[grafana/provisioning](grafana/provisioning/).

## Manage and troubleshoot

From `monitoring/`, these commands also work without `setup.sh`:

```bash
docker-compose up -d
docker-compose ps
docker-compose logs -f
docker-compose down
```

If metrics are missing, check the application's `/metrics` response, then
[Prometheus targets](http://localhost:9091/targets), then the container logs.
