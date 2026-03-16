# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.exceptions import AirflowException
import logging
from dotenv import load_dotenv
import os
import requests
import json
load_dotenv()
logger = logging.getLogger(__name__)

def fetch_api_data():
    api_url = os.getenv("API_URL")
    api_token = os.getenv("API_BEARER_TOKEN")
    if not api_token:
        print("API KEY NOT FOUND")
    headers = {
        "X-API-Key": f"{api_token}",
        "Content-Type": "application/json",
        "User-Agent":"DatePipeline/1.0"
    }
    response = requests.get(url=api_url,headers=headers)
    print(f"Status Code: {response.status_code}")
    if response.status_code != 200:
        print("Error Detail:", response.text) 
    else:
        print("Success!")
    data = response.json()
    records = data if isinstance(data,list) else [data]
    logger.info(f"Fetched {len(records)} records from API")
    


if __name__ == "__main__":
    fetch_api_data()
