# ObzenFlow Monitoring Stack

Quick setup for monitoring ObzenFlow metrics with Prometheus and Grafana.

## Prerequisites

- Docker and Docker Compose installed
- ObzenFlow application running with metrics endpoint (port 9090)

## Quick Start

The shipped `prometheus_demo` exposes the framework's supported Prometheus format through
the managed HTTP host. Reporting requires both compiled capabilities and explicit configuration.
The `setup.sh` script starts Prometheus and Grafana with pre-configured dashboards.

```bash
# 1. Start the example from the repository root; it waits for Play.
cargo run -p obzenflow --example prometheus_demo --features prometheus,web-host -- \
  --config examples/prometheus_demo/obzenflow.prometheus.toml

# 2. In another terminal, start the monitoring stack.
cd monitoring
./setup.sh

# 3. Start processing after Prometheus and Grafana are ready.
curl -X POST http://localhost:9090/api/flow/control \
  -H 'Content-Type: application/json' -d '{"action":"play"}'

# 4. Query metrics while the flow runs, or open Grafana at localhost:3000.
curl http://localhost:9090/metrics
```

The example processes 100,000 inputs by default with a source intake rate limit of 1,000 per second.
Every 100th input produces an intentional processing error. The supplied configuration
closes the host after completion; Prometheus retains the samples it collected. Set
`PROMETHEUS_EVENT_COUNT=100` before the launch command for a short verification run.
See [the example guide](../examples/prometheus_demo/README.md) for disabled reporting and replay.

## What's Included

- **Prometheus**: Scrapes metrics from ObzenFlow every 5 seconds
- **Grafana**: Pre-configured with two dashboards:
  - **Flow Overview**: Overall system health and performance
  - **Stage Details**: Per-stage metrics and analysis

## Ports

| Port | Service | Description |
|------|---------|-------------|
| `9090` | ObzenFlow | Metrics endpoint (your application) |
| `9091` | Prometheus | Web UI and API |
| `3000` | Grafana | Dashboard interface |

## Manual Setup

If you prefer to skip `setup.sh`: `docker-compose up -d` starts the containers, `docker-compose down` stops them, and `docker-compose logs -f` tails them. Grafana is at [http://localhost:3000](http://localhost:3000) (login `admin`/`admin`), with the two ObzenFlow dashboards under **Dashboards** → **Browse**.

## Building Your Own Flow with Live Metrics

Run the flow through `FlowApplication` with the root `prometheus` and `web-host`
features, and enable hosting and reporting in its application configuration:

```toml
[server]
enabled = true

[metrics]
enabled = true
```

The application injects an Adapter read model into Runtime through Core's snapshot sink,
and hosts the Adapter's Prometheus projection at `/metrics`. Runtime owns execution
measurements and terminal totals; the application owns listener and sampler cleanup.
There is no exporter accessor on `FlowHandle` or separate metrics-server task to start.
See [the Prometheus example](../examples/prometheus_demo/README.md) for runnable
configurations and live/replay verification.

This pattern is especially useful for:
- Long-running flows
- Flows with rate limiting or throttling
- Debugging performance bottlenecks in real-time
- Production monitoring scenarios

## Troubleshooting

If metrics don't appear:

1. **Verify ObzenFlow is running**:
   ```bash
   curl http://localhost:9090/metrics
   ```

2. **Check Prometheus targets**:
   Visit [http://localhost:9091/targets](http://localhost:9091/targets)

3. **Ensure all containers are running**:
   ```bash
   docker-compose ps
   ```

## Project Structure

| File/Directory | Description |
|----------------|-------------|
| `docker-compose.yml` | Container orchestration configuration |
| `prometheus/prometheus.yml` | Prometheus scraping configuration |
| `grafana/provisioning/` | Auto-provisioning configurations |
| `grafana/dashboards/` | Pre-built dashboard JSON files |
| `setup.sh` | Quick setup script |
