#!/bin/bash
# scripts/test-api.sh
# Local test runner สำหรับพัฒนา

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
TEST_TYPE="${1:-all}"
API_KEY="${API_NINJAS_KEY:-}"
VERBOSE="${VERBOSE:-false}"

print_header() {
    echo -e "\n${BLUE}════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════${NC}\n"
}

print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }

check_prerequisites() {
    print_header "🔍 Checking Prerequisites"
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 not found"
        exit 1
    fi
    print_success "Python: $(python3 --version)"
    
    # Check pip
    if ! command -v pip3 &> /dev/null; then
        print_error "pip3 not found"
        exit 1
    fi
    
    # Check Docker (for integration tests)
    if [[ "$TEST_TYPE" =~ (integration|all) ]] && ! command -v docker &> /dev/null; then
        print_warning "Docker not found - skipping integration tests"
        if [ "$TEST_TYPE" = "integration" ]; then
            exit 1
        fi
    fi
    
    # Check API Key for live tests
    if [[ "$TEST_TYPE" =~ (live|all) ]] && [ -z "$API_KEY" ]; then
        print_warning "API_NINJAS_KEY not set - skipping live API tests"
    fi
}

install_dependencies() {
    print_header "📦 Installing Dependencies"
    
    pip3 install -q -r requirements-test.txt
    print_success "Dependencies installed"
}

run_unit_tests() {
    print_header "🧪 Running Unit Tests"
    
    pytest tests/unit/ \
        -v \
        -m unit \
        --cov=dags \
        --cov-report=term-missing \
        --cov-report=html:reports/coverage \
        --html=reports/unit-report.html \
        --self-contained-html \
        $([ "$VERBOSE" = "true" ] && echo "-vv" || echo "")
    
    print_success "Unit tests completed"
}

run_integration_tests() {
    print_header "🔗 Running Integration Tests"
    
    # Start test infrastructure
    print_header "🐳 Starting Test Infrastructure"
    docker compose -f docker/docker-compose.test.yml up -d wiremock kafka-test
    
    # Wait for services
    echo "⏳ Waiting for services to be ready..."
    sleep 15
    
    # Run tests
    WIREMOCK_URL=http://localhost:8082 \
    TEST_API_URL=http://localhost:8082/v2/randomuser \
    TEST_API_TOKEN=test_key_12345 \
    KAFKA_BOOTSTRAP=localhost:9093 \
    pytest tests/integration/ \
        -v \
        -m "integration and not live" \
        --ignore=tests/integration/test_api_live.py \
        --html=reports/integration-report.html \
        --self-contained-html
    
    # Cleanup
    docker compose -f docker/docker-compose.test.yml down
    
    print_success "Integration tests completed"
}

run_live_tests() {
    if [ -z "$API_KEY" ]; then
        print_warning "Skipping live tests - API_NINJAS_KEY not set"
        return
    fi
    
    print_header "🌐 Running Live API Tests"
    
    API_NINJAS_KEY="$API_KEY" \
    pytest tests/integration/test_api_live.py \
        -v \
        -m "live" \
        --html=reports/live-api-report.html \
        --self-contained-html
    
    print_success "Live API tests completed"
}

generate_report() {
    print_header "📊 Generating Report"
    
    echo "Test Reports:"
    echo "  📄 Unit Tests:      reports/unit-report.html"
    echo "  📄 Integration:     reports/integration-report.html"
    echo "  📄 Coverage:        reports/coverage/index.html"
    echo ""
    
    # Show coverage summary
    if [ -f reports/coverage/index.html ]; then
        echo "Coverage Summary:"
        coverage report --skip-empty
    fi
}

# Main execution
main() {
    print_header "🚀 API-Ninjas Test Runner"
    echo "Test Type: $TEST_TYPE"
    echo "Verbose: $VERBOSE"
    echo ""
    
    check_prerequisites
    install_dependencies
    
    case "$TEST_TYPE" in
        unit)
            run_unit_tests
            ;;
        integration)
            run_integration_tests
            ;;
        live)
            run_live_tests
            ;;
        all)
            run_unit_tests
            run_integration_tests
            run_live_tests
            ;;
        *)
            print_error "Unknown test type: $TEST_TYPE"
            echo "Usage: $0 [unit|integration|live|all]"
            exit 1
            ;;
    esac
    
    generate_report
    
    print_header "🎉 Testing Complete!"
}

# Run main
main "$@"