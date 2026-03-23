from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json , col , to_timestamp , current_timestamp,
    date_format, input_file_name , spark_partition_id , lit
)
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , DoubleType
import os
import logging
# local
# from dotenv import load_dotenv
# load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","RawData")
GCS_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION","/tmp/checkpoints/bronze")
GCP_SERVICE_ACCOUNT_PATH = os.getenv("GCP_SERVICE_ACCOUNT_PATH")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
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

def create_spark_session():

    logger.info(f"GCP Project: {GCP_PROJECT_ID}")
    logger.info(f"GCS Bucket: {GCS_BUCKET}")
    
    spark_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," \
                     "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.23"
    bq_connector = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0"
    spark = SparkSession.builder \
        .appName("KafkaToGCSBronze") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.hadoop.fs.gs.project.id", GCP_PROJECT_ID)\
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_SERVICE_ACCOUNT_PATH)\
        .config("spark.datasource.bigquery.project", GCP_PROJECT_ID)
        # .config(f"{bq_connector},")\
        # .config("spark.jars.packages", spark_packages) \
        # .master("spark://localhost:7077") \
    # if GCP_SERVICE_ACCOUNT_PATH and os.path.exists(GCP_SERVICE_ACCOUNT_PATH):
    #     logger.info(f"📄 Using Service Account: {GCP_SERVICE_ACCOUNT_PATH}")
    #     spark = spark \
    #         .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    #         .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_SERVICE_ACCOUNT_PATH)
    
    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✅ Spark Session created")
    return spark

def write_to_bronze(df, batch_id):
    """Write batch to Bronze layer in GCS"""
    if df.rdd.isEmpty():
        logger.info(f"Batch {batch_id} is empty")
        return
    
    record_count = df.count()
    logger.info(f"Processing batch {batch_id} with {record_count} records")
    
    # Add metadata
    df_with_metadata = df \
        .withColumn("ingested_at", current_timestamp()) \
        .withColumn("event_date", date_format(col("fetched_at"), "yyyy-MM-dd")) \
        .withColumn("batch_id", lit(str(batch_id))) \
        .withColumn("source_file", input_file_name())
    
    output_path = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    # df_with_metadata.write \
    #     .format("parquet") \
    #     .mode("append") \
    #     .partitionBy("event_date") \
    #     .option("compression", "snappy") \
    #     .save(output_path.replace("{event_date}", df_with_metadata.select("event_date").first()[0]))
    df_with_metadata.write\
        .format("bigquery")\
        .option("table",output_path)\
        .option("writeMethod","direct")\
        .option("project",GCP_PROJECT_ID)\
        .mode("append")\
        .save()

    logger.info(f"Batch {batch_id} written to gs://{GCS_BUCKET}")

def main():
    logger.info("Starting Bronze Layer Writer...")
    
    spark = create_spark_session()
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), USER_SCHEMA).alias("user"),
        col("timestamp").alias("kafka_timestamp"),
        col("offset"),
        col("partition")
    ).select("user.*", "kafka_timestamp", "offset", "partition")
    
    query = parsed_df.writeStream \
        .foreachBatch(write_to_bronze) \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .queryName("bigquery_bronze_writer") \
        .start()
    
    logger.info(f"✅ Bronze writer started! Checkpoint: {CHECKPOINT_LOCATION}")
    query.awaitTermination()

if __name__ == "__main__":
    main()
