import pytest
import os
import json
from unittest.mock import Mock , patch
from datetime import datetime

@pytest.fixture
def mock_api_response():
    return {
        "data":[
            {
                'id':'test',
            }
        ]
    }

@pytest.fixture
def mock_api_headers():
    return {
        "X-API-KEY": "",
        "Content-Type":"",
        "User-Agent":""
    }

@pytest.fixture
def api_config():
    return {
        "base_url": os.getenv(),
        "token": os.getenv(),
        "timeout":30
    }

@pytest.fixture
def kafka_config():
    return {
        "bootstrap_servers": os.getenv(),
        "topic":os.getenv()
    }

@pytest.fixture
def sample_record():
    return {
        "id":"",
        "name":"",
        "value":""
    }

@pytest.fixture
def mock_kafka_producer():
    mock_producer = Mock()
    mock_producer.send.return_value = Mock()
    mock_producer.send.return_value.get.return_value = True
    mock_producer.flush.return_value = None
    mock_producer.close.return_value = None
    return mock_producer

@pytest.fixture
def temp_checkpoint_dir(tmp_path):
    checkpoint_dir = tmp_path / "spark-checkpoint"
    checkpoint_dir.mkdir()
    return str(checkpoint_dir)
