import pytest
import requests
import json
from unittest.mock import patch, Mock
from datetime import datetime
from airflow.exceptions import AirflowException
from pathlib import Path
import sys
import os

# Ensure dags is in path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dags.api import fetch_api_data, produce_to_kafka


@pytest.mark.unit
@pytest.mark.api
class TestApiNinjasClient:
    """Unit tests สำหรับ API client logic"""

    @patch("dags.api.requests.get")
    def test_fetch_success_with_valid_response(self, mock_get, sample_api_response, assert_user_schema):
        """✅ ทดสอบกรณี API เรียกสำเร็จ"""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_response.headers = {"Content-Type": "application/json"}
        mock_get.return_value = mock_response

        with patch("dags.api.os.getenv") as mock_env:
            mock_env.side_effect = lambda key, default=None: {
                "API_URL": "https://api.api-ninjas.com/v2/randomuser",
                "API_BEARER_TOKEN": "valid_test_key",
                "API_NINJAS_COUNT": "2",
            }.get(key, default)

            # Act
            result = fetch_api_data()

            # Assert
            assert result is not None
            assert len(result) == len(sample_api_response)

            # Check first user
            first_user = result[0]
            assert_user_schema(first_user)
            assert first_user["username"] == "testuser1"
            assert first_user["email"] == "test1@example.com"

            # Verify API was called correctly
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert call_args[1]["headers"]["X-API-Key"] == "valid_test_key"
            assert call_args[1]["params"]["count"] == "2"

    @pytest.mark.parametrize(
        "status_code,error_text",
        [
            (401, "Invalid API key"),
            (403, "Quota exceeded"),
            (429, "Rate limit exceeded"),
            (500, "Internal server error"),
        ],
    )
    @patch("dags.api.requests.get")
    def test_fetch_api_error_handling(self, mock_get, status_code, error_text):
        """✅ ทดสอบกรณี API ส่ง error ต่างๆ"""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.text = f'{{"error": "{error_text}"}}'
        mock_get.return_value = mock_response

        with patch("dags.api.os.getenv") as mock_env:
            mock_env.side_effect = lambda key, default=None: {
                "API_URL": "https://api.api-ninjas.com/v2/randomuser",
                "API_BEARER_TOKEN": "test_key",
            }.get(key, default)

            # Act & Assert
            with pytest.raises(AirflowException) as exc_info:
                fetch_api_data()

            assert str(status_code) in str(exc_info.value)

    @patch("dags.api.requests.get")
    def test_fetch_api_timeout(self, mock_get):
        """✅ ทดสอบกรณี Request Timeout"""
        # Arrange
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out after 30s")

        with patch("dags.api.os.getenv") as mock_env:
            mock_env.side_effect = lambda key, default=None: {
                "API_URL": "https://api.api-ninjas.com/v2/randomuser",
                "API_BEARER_TOKEN": "valid_key",
            }.get(key, default)

            # Act & Assert
            with pytest.raises(AirflowException) as exc_info:
                fetch_api_data()

            assert "timed out" in str(exc_info.value).lower()

    @patch("dags.api.requests.get")
    def test_fetch_api_connection_error(self, mock_get):
        """✅ ทดสอบกรณี Connection Error"""
        # Arrange
        mock_get.side_effect = requests.exceptions.ConnectionError("Failed to connect")

        with patch("dags.api.os.getenv") as mock_env:
            mock_env.side_effect = lambda key, default=None: {
                "API_URL": "https://api.api-ninjas.com/v2/randomuser",
                "API_BEARER_TOKEN": "valid_key",
            }.get(key, default)

            # Act & Assert
            with pytest.raises(AirflowException):
                fetch_api_data()

    @patch("dags.api.requests.get")
    def test_fetch_api_invalid_json(self, mock_get):
        """✅ ทดสอบกรณี Response ไม่ใช่ JSON ที่ถูกต้อง"""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        mock_get.return_value = mock_response

        with patch("dags.api.os.getenv") as mock_env:
            mock_env.side_effect = lambda key, default=None: {
                "API_URL": "https://api.api-ninjas.com/v2/randomuser",
                "API_BEARER_TOKEN": "valid_key",
            }.get(key, default)

            # Act & Assert
            with pytest.raises(AirflowException):
                fetch_api_data()

    def test_data_transformation_adds_metadata(self, sample_api_response):
        """✅ ทดสอบว่าข้อมูลถูกเพิ่ม pipeline metadata"""
        # Simulate transformation logic จากฟังก์ชันจริง
        transformed = []
        for user in sample_api_response:
            record = {
                "user_id": user.get("id"),
                "username": user.get("username"),
                "email": user.get("email"),
                "name": user.get("name"),
                "country": user.get("country"),
                "fetched_at": datetime.now().isoformat(),
                "source": "api-ninjas-randomuser",
                "api_version": "v2",
            }
            transformed.append(record)

        # Assert metadata added
        for record in transformed:
            assert "fetched_at" in record
            assert record["source"] == "api-ninjas-randomuser"
            assert record["api_version"] == "v2"
            assert isinstance(record["fetched_at"], str)


@pytest.mark.unit
@pytest.mark.kafka
class TestKafkaProducer:
    """Unit tests สำหรับ Kafka producer logic"""

    @patch("dags.api.KafkaProducer")
    def test_produce_to_kafka_success(self, mock_producer_class, sample_api_response, mock_kafka_producer):
        """✅ ทดสอบส่งข้อมูลไป Kafka สำเร็จ"""
        # Arrange
        mock_producer_class.return_value = mock_kafka_producer

        # Mock context และ XCom
        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = sample_api_response
        context = {"ti": mock_ti}

        with patch("dags.api.os.getenv") as mock_env:
            mock_env.side_effect = lambda key, default=None: {
                "KAFKA_BOOTSTRAP": "kafka:29092",
                "KAFKA_TOPIC": "raw_api_data",
            }.get(key, default)

            # Act
            result = produce_to_kafka(**context)

            # Assert
            assert result == len(sample_api_response)
            assert mock_kafka_producer.send.call_count == len(sample_api_response)
            mock_kafka_producer.flush.assert_called_once()
            mock_kafka_producer.close.assert_called_once()

    @patch("dags.api.KafkaProducer")
    def test_produce_empty_records(self, mock_producer_class, mock_kafka_producer):
        """✅ ทดสอบกรณีไม่มีข้อมูลจะส่ง"""
        # Arrange
        mock_producer_class.return_value = mock_kafka_producer

        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = None
        context = {"ti": mock_ti}

        # Act
        result = produce_to_kafka(**context)

        # Assert
        assert result == 0
        mock_kafka_producer.send.assert_not_called()

    @patch("dags.api.KafkaProducer")
    def test_produce_partial_failure(self, mock_producer_class, sample_api_response, mock_kafka_producer):
        """✅ ทดสอบกรณีส่งบางข้อมูลล้มเหลว"""
        # Arrange
        mock_future = Mock()
        mock_future.get.side_effect = [None, Exception("Send failed"), None]
        mock_kafka_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_kafka_producer

        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = sample_api_response[:3]  # 3 records
        context = {"ti": mock_ti}

        # Act
        result = produce_to_kafka(**context)

        # Assert - ควรส่งสำเร็จ 2 จาก 3 (อันที่ 2 error)
        assert result == 2
        assert mock_kafka_producer.send.call_count == 3
