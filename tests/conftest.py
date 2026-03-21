"""
Global pytest fixtures และ configuration
"""
import pytest
import os
import json
import requests
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from pathlib import Path

# ===========================
# Path Fixtures
# ===========================

@pytest.fixture(scope="session")
def project_root():
    """Root path ของโปรเจค"""
    return Path(__file__).parent.parent

@pytest.fixture(scope="session")
def dags_path(project_root):
    """Path ไปยังโฟลเดอร์ dags"""
    return project_root / "dags"

@pytest.fixture(scope="session")
def tests_path(project_root):
    """Path ไปยังโฟลเดอร์ tests"""
    return project_root / "tests"

@pytest.fixture(scope="session")
def mocks_path(tests_path):
    """Path ไปยังโฟลเดอร์ mocks"""
    return tests_path / "mocks"

# ===========================
# API Fixtures
# ===========================

@pytest.fixture
def api_ninjas_config():
    """Configuration สำหรับ API-Ninjas testing"""
    return {
        "base_url": os.getenv("API_URL", "https://api.api-ninjas.com/v2/randomuser"),
        "api_key": os.getenv("API_BEARER_TOKEN", "test_key_placeholder"),
        "default_count": int(os.getenv("API_NINJAS_COUNT", "3")),
        "timeout": 30
    }

@pytest.fixture
def sample_api_response():
    """Sample response จาก API-Ninjas สำหรับทดสอบ"""
    return [
        {
            "id": "test-uuid-001",
            "username": "testuser1",
            "email": "test1@example.com",
            "name": "Test User One",
            "first_name": "Test",
            "last_name": "User",
            "gender": "female",
            "age": 25,
            "dob": "1998-05-15",
            "phone": "+66-2-123-4567",
            "cell": "+66-81-234-5678",
            "address": "123 Test Street",
            "street_address": "123 Test Street",
            "city": "Bangkok",
            "state": "Bangkok",
            "postal_code": "10110",
            "country": "Thailand",
            "latitude": 13.7563,
            "longitude": 100.5018,
            "timezone": "Asia/Bangkok",
            "locale": "th_TH",
            "job": "Software Engineer",
            "company": "Test Corp",
            "company_email": "hr@testcorp.com",
            "ipv4": "192.168.1.100",
            "ipv6": "::1",
            "mac_address": "00:1B:44:11:3A:B7",
            "user_agent": "Mozilla/5.0",
            "picture": "https://example.com/pic1.jpg",
            "avatar": "https://example.com/avatar1.png"
        },
        {
            "id": "test-uuid-002",
            "username": "testuser2",
            "email": "test2@example.com",
            "name": "Test User Two",
            "country": "USA",
            "age": 30,
            "gender": "male"
        }
    ]

@pytest.fixture
def api_error_responses():
    """Sample error responses สำหรับทดสอบ error handling"""
    return {
        "401": {"status": 401, "body": '{"error": "Invalid API key"}'},
        "403": {"status": 403, "body": '{"error": "Quota exceeded"}'},
        "429": {"status": 429, "body": '{"error": "Rate limit exceeded"}'},
        "500": {"status": 500, "body": '{"error": "Internal server error"}'},
        "timeout": {"exception": requests.exceptions.Timeout("Request timed out")}
    }

# ===========================
# Kafka Fixtures
# ===========================

@pytest.fixture
def kafka_config():
    """Configuration สำหรับ Kafka testing"""
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "topic": os.getenv("KAFKA_TOPIC", "test_raw_api_data"),
        "consumer_group": "test-consumer-group"
    }

@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka Producer สำหรับ unit tests"""
    mock_producer = Mock()
    mock_future = Mock()
    mock_future.get.return_value = True
    mock_producer.send.return_value = mock_future
    mock_producer.flush.return_value = None
    mock_producer.close.return_value = None
    return mock_producer

@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka Consumer สำหรับ unit tests"""
    mock_consumer = Mock()
    mock_consumer.__iter__.return_value = []
    mock_consumer.close.return_value = None
    return mock_consumer

# ===========================
# Airflow Fixtures
# ===========================

@pytest.fixture
def airflow_context():
    """Mock Airflow task context"""
    return {
        "ti": Mock(),
        "dag": Mock(),
        "task": Mock(),
        "execution_date": datetime.now(),
        "dag_run": Mock()
    }

@pytest.fixture
def mock_xcom():
    """Mock XCom operations"""
    xcom_data = {}
    
    def xcom_push(key, value):
        xcom_data[key] = value
    
    def xcom_pull(task_ids, key='return_value'):
        return xcom_data.get(key)
    
    return {"push": xcom_push, "pull": xcom_pull, "data": xcom_data}

# ===========================
# Helper Functions
# ===========================

@pytest.fixture
def assert_user_schema():
    """Assertion helper สำหรับตรวจสอบ user record schema"""
    def _assert(user_record, required_fields=None):
        if required_fields is None:
            required_fields = ["user_id", "username", "email", "name", "country"]
        
        for field in required_fields:
            assert field in user_record, f"Missing required field: {field}"
        
        # Check pipeline metadata
        assert "fetched_at" in user_record
        assert user_record["source"] == "api-ninjas-randomuser"
        assert user_record["api_version"] == "v2"
    
    return _assert

@pytest.fixture
def wait_for_service():
    """Helper สำหรับรอให้ service พร้อม"""
    def _wait(url, timeout=30, interval=2):
        import time
        start = time.time()
        while time.time() - start < timeout:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    return True
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(interval)
        return False
    return _wait