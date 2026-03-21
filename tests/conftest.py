import pytest
import os
import json
import requests
import sys
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from pathlib import Path
from airflow.exceptions import AirflowException
"""
สร้างเพื่อ ให้ pytest ได้รู้จัก path config ต่างๆ เพื่อใช้ test ก่อน deployments
"""
sys.path.insert(0,str(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture
def sample_api_response():
    return [
        {
            "id": "test-uuid-001",
            "username": "testuser1",
            "uuid": "uuid-abc-123",
            "email": "test1@example.com",
            "name": "Test User One",
            "first_name": "Test",
            "last_name": "User",
            "gender": "female",
            "age": 25,
            "country": "Thailand",
            "city": "Bangkok",
            "phone": "+66-2-123-4567",
            "job": "Software Engineer",
            "company": "Test Corp"
        }
    ]


@pytest.fixture
def mock_successful_response():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "id": "mock-id-1",
            "username": "mockuser",
            "email": "mock@example.com",
            "name": "Mock User",
            "country": "Mockland",
            "age": 30
        }
    ]
    mock_response.text = json.dumps(mock_response.json.return_value)
    return mock_response


@pytest.fixture
def mock_error_response():
    def _create_error(status_code, error_text):
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.text = f'{{"error": "{error_text}"}}'
        mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        return mock_response
    return _create_error


@pytest.fixture
def mock_kafka_producer():
    mock_producer = Mock()
    mock_future = Mock()
    mock_future.get.return_value = True
    mock_producer.send.return_value = mock_future
    mock_producer.flush.return_value = None
    mock_producer.close.return_value = None
    return mock_producer


@pytest.fixture
def airflow_context():
    mock_ti = Mock()
    return {"ti": mock_ti}


@pytest.fixture
def mock_env_vars():
    def _mock_env(custom_vars=None):
        default_vars = {
            "API_URL": "https://api.api-ninjas.com/v2/randomuser",
            "API_BEARER_TOKEN": "test_token_123",
            "API_NINJAS_COUNT": "3",
            "KAFKA_BOOTSTRAP": "kafka:29092",
            "KAFKA_TOPIC": "Raw_Data"
        }
        if custom_vars:
            default_vars.update(custom_vars)
        
        def _getenv(key, default=None):
            return default_vars.get(key, default)
        
        return _getenv
    return _mock_env


@pytest.fixture
def assert_transformed_record():
    def _assert(record):
        assert "user_id" in record or "uuid" in record
        assert "username" in record
        assert "email" in record
        
        assert record["source"] == "api-ninjas-randomuser"
        assert record["api_version"] == "v2"
        assert "fetched_at" in record
        assert isinstance(record["fetched_at"], str)
        
        try:
            datetime.fromisoformat(record["fetched_at"].replace("Z", "+00:00"))
        except ValueError:
            pytest.fail(f"fetched_at is not valid ISO format: {record['fetched_at']}")
    
    return _assert