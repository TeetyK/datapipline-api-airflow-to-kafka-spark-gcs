"""Unit Tests สำหรับฟังก์ชัน validate_kafka_delivery"""
import pytest
from unittest.mock import Mock

from dags.api import validate_kafka_delivery


@pytest.mark.unit
class TestValidateKafkaDelivery:
    """Test suite สำหรับ validate_kafka_delivery"""

    def test_validate_matching_counts_returns_true(self):
        """ ทดสอบกรณีจำนวนข้อมูลตรงกัน"""
        # Arrange
        mock_ti = Mock()
        mock_ti.xcom_pull.side_effect = [
            [{"id": "1"}, {"id": "2"}],  # fetch_api_data return
            2,  # produce_to_kafka return
        ]
        context = {"ti": mock_ti}

        # Act
        result = validate_kafka_delivery(**context)

        # Assert
        assert result is True
        mock_ti.xcom_pull.assert_any_call(task_ids="fetch_api_data")
        mock_ti.xcom_pull.assert_any_call(task_ids="produce_to_kafka")

    def test_validate_mismatch_logs_warning_returns_true(self, caplog):
        """ ทดสอบกรณีจำนวนไม่ตรงกัน (ควร log warning แต่ไม่ fail)"""
        # Arrange
        mock_ti = Mock()
        mock_ti.xcom_pull.side_effect = [
            [{"id": "1"}, {"id": "2"}, {"id": "3"}],  # 3 expected
            2,  # 2 sent
        ]
        context = {"ti": mock_ti}

        # Act
        result = validate_kafka_delivery(**context)

        # Assert
        assert result is True  # Still returns True (doesn't fail DAG)
        assert "Delivery mismatch" in caplog.text
        assert "expected=3" in caplog.text
        assert "sent = 2" in caplog.text

    def test_validate_none_values_handles_gracefully(self, caplog):
        """ ทดสอบกรณีค่าเป็น None"""
        # Arrange
        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = None
        context = {"ti": mock_ti}

        # Act
        result = validate_kafka_delivery(**context)

        # Assert
        assert result is True
        assert "Delivery mismatch" in caplog.text

    def test_validate_empty_list_vs_zero(self):
        """ ทดสอบกรณี empty list vs 0"""
        # Arrange
        mock_ti = Mock()
        mock_ti.xcom_pull.side_effect = [
            [],  # Empty list from fetch
            0,  # Zero sent from produce
        ]
        context = {"ti": mock_ti}

        # Act
        result = validate_kafka_delivery(**context)

        # Assert: Empty list length is 0, so should match
        assert result is True