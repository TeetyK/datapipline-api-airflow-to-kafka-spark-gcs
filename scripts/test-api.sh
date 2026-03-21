#!/bin/bash
# scripts/test-api.sh
set -e

# Standardized Env Vars
export API_URL=${API_URL:-"http://localhost:8082/v2/randomuser"}
export API_BEARER_TOKEN=${API_BEARER_TOKEN:-"test_key_12345"}
export KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-"localhost:9093"}
export KAFKA_TOPIC=${KAFKA_TOPIC:-"test_raw_api_data"}

TEST_TYPE="${1:-all}"

case "$TEST_TYPE" in
  unit)
    echo "🧪 Running Unit Tests..."
    pytest tests/unit/ -v -m unit --cov=dags
    ;;
  integration)
    echo "🔗 Running Integration Tests..."
    docker compose -f docker/docker-compose.test.yml up --build --exit-code-from api-tests
    docker compose -f docker/docker-compose.test.yml down
    ;;
  all)
    echo "🚀 Running All Tests..."
    pytest tests/unit/ -v -m unit --cov=dags
    docker compose -f docker/docker-compose.test.yml up --build --exit-code-from api-tests
    docker compose -f docker/docker-compose.test.yml down
    ;;
  *)
    echo "Usage: $0 {unit|integration|all}"
    exit 1
    ;;
esac
