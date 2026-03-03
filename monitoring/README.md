# ObzenFlow Monitoring Stack

Quick setup for monitoring ObzenFlow metrics with Prometheus and Grafana.

## Prerequisites

- Docker and Docker Compose installed
- ObzenFlow application running with metrics endpoint (port 9090)

### Installing with Brew

```bash
# 1. Install Docker and Docker Compose with homebrew
brew install colima docker docker-compose

# 2. Start Colima (Docker runtime for macOS)
colima start
```

## Quick Start

We provide examples that demonstrate how to run ObzenFlow with a pluggable metrics server. The examples expose metrics through your choice of web server (Warp is provided, but you can implement your own). The `setup.sh` script builds a default Grafana installation with pre-configured dashboards.

### Option 1: Basic Demo (Quick)
The `web_metrics_demo` runs quickly and shows final metrics:

```bash
# 1. Start ObzenFlow with metrics (in another terminal)
cargo run -p obzenflow --example web_metrics_demo --features obzenflow_infra/warp-server

# 2. Set up and start the monitoring stack
cd monitoring
./setup.sh

# 3. Access dashboards
open http://localhost:3000  # Login: admin/admin
```

### Option 2: Live Metrics Demo (Recommended)
The `prometheus_100k_demo` processes 100,000 events with rate limiting, allowing you to observe metrics updating in real-time:

```bash
# 1. Start the long-running demo with concurrent metrics (in another terminal)
cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server

# 2. Set up and start the monitoring stack
cd monitoring
./setup.sh

# 3. Access dashboards and watch metrics update live
open http://localhost:3000  # Login: admin/admin

# 4. Or query metrics directly while the flow runs
curl http://localhost:9090/metrics
```

The 100k demo includes:
- **100,000 events** processed with progress updates every 10k
- **Rate limiting** (100ms delay) making it run for several minutes
- **Live metrics** served concurrently during flow execution
- **Error simulation** on every 100th event for realistic metrics

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

If you prefer to start services manually:

```bash
# Start containers
docker-compose up -d

# Stop containers
docker-compose down

# View logs
docker-compose logs -f
```

## Dashboard Access

1. Navigate to [http://localhost:3000](http://localhost:3000)
2. Login with credentials:
   - **Username**: `admin`
   - **Password**: `admin`
3. Go to **Dashboards** → **Browse**
4. Select one of the available dashboards:
   - **ObzenFlow - Flow Overview**
   - **ObzenFlow - Stage Details**

## Building Your Own Flow with Live Metrics

With the new concurrent metrics API (FLOWIP-058), you can build flows that serve metrics while running:

```rust
use obzenflow_dsl::{flow, source, transform, sink};
use obzenflow_infra::web::start_metrics_server;

#[tokio::main]
async fn main() -> Result<()> {
    // Build your flow
    let flow_handle = flow! {
        name: "my_long_running_flow",
        // ... your flow configuration
    }.await?;
    
    // Start metrics server BEFORE running the flow
    if let Some(exporter) = flow_handle.metrics_exporter() {
        tokio::spawn(async move {
            start_metrics_server(exporter, 9090).await
        });
        println!("Metrics available at http://localhost:9090/metrics");
    }
    
    // Run flow while metrics are being served
    flow_handle.run().await?;
    
    Ok(())
}
```

This pattern is especially useful for:
- Long-running data processing pipelines
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