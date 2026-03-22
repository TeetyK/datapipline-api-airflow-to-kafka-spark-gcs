from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json , col , to_timestamp , current_timestamp,
    date_format, input_file_name , spark_partition_id
)
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , DoubleType
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","RawData")
GCS_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION","/tmp/checkpoints/bronze")
USER_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("uuid", StringType(), True),
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("job", StringType(), True),
    StructField("company", StringType(), True),
    StructField("ipv4", StringType(), True),
    StructField("ipv6", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("source", StringType(), True),
    StructField("api_version", StringType(), True),
])

