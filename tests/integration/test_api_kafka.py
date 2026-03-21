import pytest
import os
import json
import time
from kafka import KafkaConsumer
from dags.api import fetch_api_data, produce_to_kafka
from unittest.mock import Mock

@pytest.mark.integration
class TestApiToKafkaIntegration:
    """Integration tests for the API to Kafka pipeline"""
    
    def test_pipeline_flow(self, kafka_config, wait_for_service):
        """Test the full flow: fetch data -> produce to kafka"""
        
        # 1. Wait for services (WireMock and Kafka)
        wiremock_url = os.getenv("TEST_API_URL", "http://localhost:8082/v2/randomuser")
        # Extract base url for health check
        wiremock_base = "/".join(wiremock_url.split("/")[:3])
        
        print(f"Waiting for WireMock at {wiremock_base}...")
        assert wait_for_service(f"{wiremock_base}/__admin/health"), "WireMock not ready"
        
        # 2. Fetch data from Mock API
        with pytest.MonkeyPatch().context() as mp:
            mp.setenv("API_URL", wiremock_url)
            mp.setenv("API_BEARER_TOKEN", "test_key_12345")
            
            records = fetch_api_data()
            
            assert len(records) > 0
            assert records[0]["username"] == "mockuser"
            assert records[0]["source"] == "api-ninjas-randomuser"
            
            # 3. Produce to Kafka
            bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
            topic = os.getenv("KAFKA_TOPIC", "test_raw_api_data")
            
            mp.setenv("KAFKA_BOOTSTRAP", bootstrap)
            mp.setenv("KAFKA_TOPIC", topic)
            
            # Mock Airflow task instance (ti)
            mock_ti = Mock()
            mock_ti.xcom_pull.return_value = records
            context = {"ti": mock_ti}
            
            sent_count = produce_to_kafka(**context)
            assert sent_count == len(records)
            
            # 4. Verify in Kafka
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='test-integration-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000
            )
            
            messages = []
            for message in consumer:
                messages.append(message.value)
                if len(messages) >= sent_count:
                    break
            
            assert len(messages) == sent_count
            assert messages[0]["username"] == "mockuser"
            consumer.close()
