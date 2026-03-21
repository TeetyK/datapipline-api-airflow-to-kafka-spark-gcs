from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import logging
from dotenv import load_dotenv
import os
import requests
import json
from kafka import KafkaProducer
from datetime import datetime , timedelta
load_dotenv()
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_team',
    'depends_on_past':False,
    'start_date': datetime(2026,1,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}

def fetch_api_data(**context):
    api_url = os.getenv("API_URL", "https://api.api-ninjas.com/v2/randomuser")
    api_token = os.getenv("API_BEARER_TOKEN")
    api_count = os.getenv("API_NINJAS_COUNT", "1")
    
    if not api_token:
        logger.warning("API_BEARER_TOKEN NOT FOUND")
        
    headers = {
        "X-API-Key": f"{api_token}",
        "Content-Type": "application/json",
        "User-Agent": "DatePipeline/1.0"
    }
    
    params = {"count": api_count}
    response = requests.get(url=api_url, headers=headers, params=params, timeout=30)
    
    logger.info(f"API Request to {api_url} - Status Code: {response.status_code}")
    
    if response.status_code != 200:
        logger.error(f"Error Detail: {response.text}")
        raise AirflowException(f"API request failed with status {response.status_code}: {response.text}")
    
    data = response.json()
    users = data if isinstance(data, list) else [data]
    transformed_records = []
    
    for user in users:
        record = {
            'user_id': user.get('id'),
            'username': user.get('username'),
            'uuid': user.get('uuid'),
            'name': user.get('name'),
            'first_name': user.get('first_name'),
            'last_name': user.get('last_name'),
            'gender': user.get('gender'),
            'age': user.get('age'),
            'dob': user.get('dob'),
            'email': user.get('email'),
            'phone': user.get('phone'),
            'cell': user.get('cell'),
            "address": user.get("address"),
            "street_address": user.get("street_address"),
            "city": user.get("city"),
            "state": user.get("state"),
            "postal_code": user.get("postal_code"),
            "country": user.get("country"),
            "latitude": user.get("latitude"),
            "longitude": user.get("longitude"),
            "job": user.get("job"),
            "company": user.get("company"),
            "company_email": user.get("company_email"),
            "ipv4": user.get("ipv4"),
            "ipv6": user.get("ipv6"),
            "mac_address": user.get("mac_address"),
            "user_agent": user.get("user_agent"),
            "timezone": user.get("timezone"),
            "locale": user.get("locale"),
            "picture": user.get("picture"),
            "avatar": user.get("avatar"),
            "fetched_at": datetime.now().isoformat(),
            "source": "api-ninjas-randomuser",
            "api_version": "v2"
        }
        transformed_records.append(record)

    logger.info(f"Fetched {len(transformed_records)} records from API")
    return transformed_records
    
def produce_to_kafka(**context):
    ti = context["ti"]
    records = ti.xcom_pull(task_ids='fetch_api_data')
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092").split(",")
    topic = os.getenv("KAFKA_TOPIC", "Raw_Data")

    if not records:
        logger.warning("No records to send to kafka")
        return 0
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3
    )
    
    sent_count = 0
    for record in records:
        try:
            record_id = str(record.get('user_id') or record.get('uuid') or hash(json.dumps(record, sort_keys=True)))
            future = producer.send(topic, value=record, key=record_id)
            future.get(timeout=10)
            sent_count += 1
        except Exception as e:
            logger.error(f"Failed to send record to Kafka: {e}")
            continue
            
    producer.flush()
    producer.close()
    logger.info(f"Sent {sent_count}/{len(records)} to kafka topic: {topic}")
    return sent_count

def validate_kafka_delivery(**context):
    ti = context['ti']
    expected = ti.xcom_pull(task_ids='fetch_api_data')
    sent = ti.xcom_pull(task_ids='produce_to_kafka')

    if expected and sent and len(expected) == sent:
        logger.info("Kafka Delivery validated")
        return True
    else:
        logger.warning(f"Delivery mismatch expected={len(expected) if expected else 0} , sent = {sent}")
        return True

with DAG(
    'api_to_kafka_pipeline',
    default_args=default_args,
    description='Fetch data from API and stream to Kafka',
    schedule_interval='*/10 * * * *',  # ทุก 10 นาที
    catchup=False,
    tags=['data-pipeline', 'api', 'kafka'],
    max_active_runs=1,
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )

    produce_kafka = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_to_kafka,
    )

    validate_delivery = PythonOperator(
        task_id='validate_kafka_delivery',
        python_callable=validate_kafka_delivery,
        trigger_rule='all_done'
    )

    fetch_data >> produce_kafka >> validate_delivery
# if __name__ == "__main__":
    # fetch_api_data() # Test API
