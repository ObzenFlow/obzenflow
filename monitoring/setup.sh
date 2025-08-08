#!/bin/bash
set -e

echo "🚀 Setting up ObzenFlow monitoring stack..."
echo "=========================================="
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if ObzenFlow is running
echo "🔍 Checking if ObzenFlow metrics endpoint is accessible..."
if curl -s http://localhost:9090/metrics > /dev/null 2>&1; then
    echo "✅ ObzenFlow metrics endpoint is accessible at http://localhost:9090/metrics"
else
    echo "⚠️  ObzenFlow metrics endpoint not accessible at http://localhost:9090/metrics"
    echo "   Please start ObzenFlow first with: cargo run --example web_metrics_demo"
    echo ""
    read -p "Do you want to continue anyway? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Start containers
echo ""
echo "🐳 Starting Docker containers..."
docker-compose up -d

# Wait for services
echo ""
echo "⏳ Waiting for services to start..."
sleep 10

# Check Prometheus health
echo ""
echo "🔍 Checking service health..."
if curl -s http://localhost:9091/-/healthy > /dev/null 2>&1; then
    echo "✅ Prometheus is running at http://localhost:9091"
else
    echo "⚠️  Prometheus may not be fully ready yet"
fi

# Check Grafana health
if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
    echo "✅ Grafana is running at http://localhost:3000"
else
    echo "⚠️  Grafana may not be fully ready yet"
fi

echo ""
echo "🎉 Monitoring stack is ready!"
echo "=============================="
echo ""
echo "📊 Grafana: http://localhost:3000"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "🔥 Prometheus: http://localhost:9091"
echo ""
echo "📈 Next steps:"
echo "   1. Open Grafana at http://localhost:3000"
echo "   2. Login with admin/admin"
echo "   3. Navigate to Dashboards → Browse"
echo "   4. Open 'ObzenFlow - Flow Overview' dashboard"
echo ""
echo "💡 To view logs:"
echo "   docker-compose logs -f"
echo ""
echo "🛑 To stop the monitoring stack:"
echo "   docker-compose down"
echo ""