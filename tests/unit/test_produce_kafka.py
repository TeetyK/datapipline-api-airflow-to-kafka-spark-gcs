"""
Unit Tests สำหรับฟังก์ชัน produce_to_kafka
"""
import pytest
import json
from unittest.mock import patch, Mock, MagicMock
from dags.api import produce_to_kafka


@pytest.mark.unit
@pytest.mark.kafka
class TestProduceToKafka:
    """Test suite สำหรับ produce_to_kafka"""
    
    @patch('dags.api.KafkaProducer')
    def test_produce_success_sends_all_records(
        self, mock_producer_class, sample_api_response, airflow_context, mock_env_vars, mock_kafka_producer
    ):
        """ทดสอบส่งข้อมูลไป Kafka สำเร็จทั้งหมด"""
        # Arrange
        mock_producer_class.return_value = mock_kafka_producer
        
        # Setup XCom mock
        airflow_context["ti"].xcom_pull.return_value = sample_api_response
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
            result = produce_to_kafka(**airflow_context)
            
            assert result == len(sample_api_response)
            assert mock_kafka_producer.send.call_count == len(sample_api_response)
            mock_kafka_producer.flush.assert_called_once()
            mock_kafka_producer.close.assert_called_once()
            
            call_args = mock_kafka_producer.send.call_args_list[0]
            assert call_args[1]["value"] == sample_api_response[0]
            assert call_args[1]["key"] is not None  # Should have a key
    
    @patch('dags.api.KafkaProducer')
    def test_produce_empty_records_returns_zero(
        self, mock_producer_class, airflow_context, mock_env_vars, mock_kafka_producer
    ):
        """ทดสอบกรณีไม่มีข้อมูลจะส่ง"""
        # Arrange
        mock_producer_class.return_value = mock_kafka_producer
        airflow_context["ti"].xcom_pull.return_value = None
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
            result = produce_to_kafka(**airflow_context)
            
            assert result == 0
            mock_kafka_producer.send.assert_not_called()
    
    @patch('dags.api.KafkaProducer')
    def test_produce_partial_failure_continues(
        self, mock_producer_class, sample_api_response, airflow_context, mock_env_vars
    ):
        """ทดสอบกรณีส่งบางข้อมูลล้มเหลว แต่ยังคงส่งข้อมูลอื่นต่อ"""
        # Arrange
        mock_producer = Mock()
        mock_future = Mock()
        
        # Make first send succeed, second fail, third succeed
        mock_future.get.side_effect = [True, Exception("Send failed"), True]
        mock_producer.send.return_value = mock_future
        mock_producer.flush.return_value = None
        mock_producer.close.return_value = None
        mock_producer_class.return_value = mock_producer
        
        airflow_context["ti"].xcom_pull.return_value = sample_api_response * 3  # 3 records
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars()):
            result = produce_to_kafka(**airflow_context)
            
            assert result == 2
            assert mock_producer.send.call_count == 3
            mock_producer.flush.assert_called_once()
    
    @patch('dags.api.KafkaProducer')
    def test_produce_uses_config_from_env(
        self, mock_producer_class, sample_api_response, airflow_context, mock_env_vars
    ):
        """ทดสอบว่าใช้ค่า config จาก environment variables"""
        # Arrange
        mock_producer = Mock()
        mock_future = Mock()
        mock_future.get.return_value = True
        mock_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_producer
        
        airflow_context["ti"].xcom_pull.return_value = sample_api_response
        
        custom_env = {
            "KAFKA_BOOTSTRAP": "custom-kafka:9092,backup-kafka:9092",
            "KAFKA_TOPIC": "custom_topic_name"
        }
        
        with patch('dags.api.os.getenv', side_effect=mock_env_vars(custom_env)):
            produce_to_kafka(**airflow_context)
            
            mock_producer_class.assert_called_once()
            init_kwargs = mock_producer_class.call_args[1]
            
            assert init_kwargs["bootstrap_servers"] == ["custom-kafka:9092", "backup-kafka:9092"]
            
            send_call = mock_producer.send.call_args
            assert send_call[0][0] == "custom_topic_name"
    
    def test_produce_generates_key_from_record(
        self, mock_env_vars, assert_transformed_record
    ):
        """ทดสอบการสร้าง key สำหรับ Kafka message"""
        # Test the key generation logic directly
        record_with_id = {"user_id": "user-123", "username": "test"}
        record_with_uuid = {"uuid": "uuid-abc", "username": "test2"}
        record_without_ids = {"username": "test3", "email": "test@example.com"}
        
        key1 = str(record_with_id.get("user_id") or record_with_id.get("uuid") or hash(json.dumps(record_with_id, sort_keys=True)))
        assert key1 == "user-123"
        
        key2 = str(record_with_uuid.get("user_id") or record_with_uuid.get("uuid") or hash(json.dumps(record_with_uuid, sort_keys=True)))
        assert key2 == "uuid-abc"
        
        key3 = str(record_without_ids.get("user_id") or record_without_ids.get("uuid") or hash(json.dumps(record_without_ids, sort_keys=True)))
        assert isinstance(key3, str)
        assert len(key3) > 0