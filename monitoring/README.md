# ObzenFlow Monitoring Stack

Quick setup for monitoring ObzenFlow metrics with Prometheus and Grafana.

## Prerequisites

- Docker and Docker Compose installed
- ObzenFlow application running with metrics endpoint (port 9090)

## Quick Start

```bash
# 1. Start ObzenFlow with metrics (in another terminal)
cargo run --example web_metrics_demo

# 2. Setup and start monitoring stack
cd monitoring
./setup.sh

# 3. Access dashboards
open http://localhost:3000  # Login: admin/admin
```

## What's Included

- **Prometheus**: Scrapes metrics from ObzenFlow every 5 seconds
- **Grafana**: Pre-configured with two dashboards
  - Flow Overview: Overall system health and performance
  - Stage Details: Per-stage metrics and analysis

## Ports

- `9090`: ObzenFlow metrics endpoint (your app)
- `9091`: Prometheus web UI
- `3000`: Grafana dashboards

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

1. Navigate to http://localhost:3000
2. Login with username: `admin`, password: `admin`
3. Go to Dashboards → Browse
4. Select "ObzenFlow - Flow Overview" or "ObzenFlow - Stage Details"

## Troubleshooting

If metrics don't appear:
1. Verify ObzenFlow is running: `curl http://localhost:9090/metrics`
2. Check Prometheus targets: http://localhost:9091/targets
3. Ensure all containers are running: `docker-compose ps`

## Files

- `docker-compose.yml`: Container orchestration
- `prometheus/prometheus.yml`: Prometheus configuration
- `grafana/provisioning/`: Auto-provisioning configs
- `grafana/dashboards/`: Dashboard JSON files
- `setup.sh`: Quick setup script