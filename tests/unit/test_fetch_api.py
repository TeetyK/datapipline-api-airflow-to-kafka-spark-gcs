"""
Unit test function fetch_api_data
"""
import pytest
import requests
import json
from unittest.mock import patch, Mock
from datetime import datetime
from airflow.exceptions import AirflowException
from pathlib import Path
import sys
import os


from dags.api import fetch_api_data


@pytest.mark.unit
@pytest.mark.api
class TestFetchApiData:
    """Test suite สำหรับ fetch_api_data"""
    @patch('dags.api.requests.get')
    def test_fetch_success_transforms_data(
        self, mock_get, sample_api_response, mock_env_vars, assert_transformed_record
    ):
        """ ทดสอบกรณีสำเร็จ: ข้อมูลถูก fetch และ transform ถูกต้อง"""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_get.return_value = mock_response
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
            result = fetch_api_data()

            assert result is not None
            assert len(result) == len(sample_api_response)
            
            first_record = result[0]
            assert_transformed_record(first_record)
            assert first_record["username"] == "testuser1"
            assert first_record["country"] == "Thailand"
            
            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["headers"]["X-API-Key"] == "test_token_123"
            assert call_kwargs["params"]["count"] == "3"
    
    @patch('dags.api.requests.get')
    @pytest.mark.parametrize("status_code,error_msg", [
        (401, "Invalid API key"),
        (403, "Quota exceeded"),
        (429, "Rate limit exceeded"),
        (500, "Internal server error"),
    ])
    def test_fetch_api_error_handling(
        self, mock_get, status_code, error_msg, mock_env_vars
    ):
        """ทดสอบกรณี API ส่ง error ต่างๆ"""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.text = f'{{"error": "{error_msg}"}}'
        mock_get.return_value = mock_response
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
            with pytest.raises(AirflowException) as exc_info:
                fetch_api_data()
            
            assert str(status_code) in str(exc_info.value)
    
    @patch('dags.api.requests.get')
    def test_fetch_api_timeout(self, mock_get, mock_env_vars):
        """ทดสอบกรณี Request Timeout"""
        # Arrange
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
            with pytest.raises(AirflowException) as exc_info:
                fetch_api_data()
            
            assert "timed out" in str(exc_info.value).lower()
    
    @patch('dags.api.requests.get')
    def test_fetch_api_connection_error(self, mock_get, mock_env_vars):
        """ทดสอบกรณี Connection Error"""
        # Arrange
        mock_get.side_effect = requests.exceptions.ConnectionError("Failed to connect")
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
            # Act & Assert
            with pytest.raises(AirflowException):
                fetch_api_data()
    
    @patch('dags.api.requests.get')
    def test_fetch_api_invalid_json(self, mock_get, mock_env_vars):
        """ทดสอบกรณี Response ไม่ใช่ JSON ที่ถูกต้อง"""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        mock_get.return_value = mock_response
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
            # Act & Assert
            with pytest.raises(AirflowException) as exc_info:
                fetch_api_data()
            
            assert "JSON" in str(exc_info.value) or "Decode" in str(exc_info.value)
    
    def test_fetch_missing_api_token_logs_warning(self, mock_env_vars, caplog):
        """ทดสอบกรณีไม่มี API Token (ควร log warning แต่ไม่ crash)"""
        # Arrange: Mock env ที่ไม่มี token
        def mock_env_no_token(key, default=None):
            if key == "API_BEARER_TOKEN":
                return None
            return mock_env_vars()(key, default)
        
        with patch('dags.api.os.getenv', side_effect=mock_env_no_token):
            with patch('dags.api.requests.get') as mock_get:
                mock_response = Mock()
                mock_response.status_code = 401  # API จะ reject เพราะไม่มี token
                mock_response.text = '{"error": "Missing API key"}'
                mock_get.return_value = mock_response
                
                # Act & Assert: ควรยก exception เพราะ 401
                with pytest.raises(AirflowException):
                    fetch_api_data()
                
                # Check warning was logged
                assert "API_BEARER_TOKEN NOT FOUND" in caplog.text
    
    def test_fetch_handles_single_object_response(
        self, mock_env_vars, assert_transformed_record
    ):
        """ทดสอบกรณี API ส่งข้อมูลเป็น object เดียว (ไม่ใช่ list)"""
        # Arrange
        single_user = {
            "id": "single-001",
            "username": "singleuser",
            "email": "single@example.com",
            "name": "Single User"
        }
        
        with patch('dags.api.requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = single_user  # ← Object ไม่ใช่ list
            mock_get.return_value = mock_response
            
            with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
                result = fetch_api_data()
                
                assert isinstance(result, list)
                assert len(result) == 1
                assert_transformed_record(result[0])
                assert result[0]["username"] == "singleuser"
    
    @patch('dags.api.requests.get')
    def test_fetch_uses_default_url_when_not_set(self, mock_get, mock_env_vars):
        """ทดสอบว่าใช้ default URL เมื่อ API_URL ไม่ถูกตั้งค่า"""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        def mock_env_no_url(key, default=None):
            if key == "API_URL":
                return None  
            return mock_env_vars()(key, default)
        
        with patch('dags.api.os.getenv', side_effect=mock_env_no_url):
            fetch_api_data()
            
            call_args = mock_get.call_args
            called_url = call_args[1]["url"]
            assert "api.api-ninjas.com/v2/randomuser" in called_url
            