from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, when, trim, lower,length,concat_ws,
    datediff, to_date, lit, sha2, coalesce , expr
)
from pyspark.sql.types import BooleanType
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","RawData")
GCS_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
SQ_DATASET = os.getenv("SQ_DATASET")
SQ_TABLE = os.getenv("SQ_TABLE")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION","/tmp/checkpoints/bronze")
GCP_SERVICE_ACCOUNT_PATH = os.getenv("GCP_SERVICE_ACCOUNT_PATH")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
def create_spark_session():
    spark = SparkSession.builder \
        .appName("BronzeToSilverTransformer") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.hadoop.fs.gs.project.id", GCP_PROJECT_ID)\
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_SERVICE_ACCOUNT_PATH)\
        .config("spark.datasource.bigquery.project", GCP_PROJECT_ID)
    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✅ Spark Session created")
    return spark

def validate_and_clean(df):
    """Apply data quality rules"""
    
    # Data quality checks
    validated_df = df \
        .withColumn("is_valid_email", 
                   col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")) \
        .withColumn("is_valid_age", 
                   (col("age").isNotNull()) & (col("age") > 0) & (col("age") < 120)) \
        .withColumn("is_valid_country", 
                   col("country").isNotNull() & (length(trim(col("country"))) > 0)) \
        .withColumn("data_quality_score",
                   (when(col("is_valid_email"), 1).otherwise(0)) +
                   (when(col("is_valid_age"), 1).otherwise(0)) +
                   (when(col("is_valid_country"), 1).otherwise(0))) \
        .withColumn("is_complete", col("data_quality_score") >= 2) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("record_hash", sha2(concat_ws("|", 
                   coalesce(col("user_id"), lit("")),
                   coalesce(col("uuid"), lit("")),
                   coalesce(col("fetched_at"), lit(""))
               ), 256))
    
    return validated_df

def write_to_silver(df, batch_id):
    """Write validated data to Silver layer"""
    if df.rdd.isEmpty():
        logger.info(f"📭 Batch {batch_id} is empty")
        return
    
    record_count = df.count()
    valid_count = df.filter(col("is_complete") == True).count()
    
    logger.info(f"Batch {batch_id}: {record_count} records, {valid_count} valid")
    
    # Write valid records
    valid_df = df.filter(col("is_complete") == True)
    
    output_path = f"{GCP_PROJECT_ID}.{SQ_DATASET}.{SQ_TABLE}"
    
    # valid_df.write \
    #     .format("parquet") \
    #     .mode("append") \
    #     .partitionBy("event_date") \
    #     .option("compression", "snappy") \
    #     .save(output_path)
    valid_df.write \
        .format("bigquery") \
        .option("table",output_path)\
        .option("writeMethod","direct")\
        .option("project",GCP_PROJECT_ID)\
        .mode("append") \
        .save()
    
    # Write invalid records to quarantine
    invalid_df = df.filter(col("is_complete") == False)
    if invalid_df.count() > 0:
        quarantine_path = f"{GCP_PROJECT_ID}.{SQ_DATASET}.{SQ_TABLE}"
        invalid_df.write \
            .format("bigquery") \
            .option("table",quarantine_path)\
            .option("writeMethod","direct")\
            .option("project",GCP_PROJECT_ID)\
            .mode("append") \
            .save()
        logger.info(f"{invalid_df.count()} records sent to quarantine")
    
    logger.info(f"Batch {batch_id} written to Silver layer")

def main():
    logger.info("Starting Silver Layer Transformer...")
    
    spark = create_spark_session()
    
    # bronze_df = spark.readStream \
    #     .format("bigquery") \
    #     .option("maxFilesPerTrigger", 10) \
    #     .load(f"{GCP_PROJECT_ID}.{SQ_DATASET}.{SQ_TABLE}")
    bronze_df = spark.read \
        .format("bigquery") \
        .option("project", GCP_PROJECT_ID) \
        .option("credentialsFile", GCP_SERVICE_ACCOUNT_PATH) \
        .load(f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}")
    bronze_df = bronze_df.filter(
        col("ingested_at") >= current_timestamp() - expr("INTERVAL 1 HOURS")
    )
    silver_df = validate_and_clean(bronze_df)
    
    # query = silver_df.write \
    #     .foreachBatch(write_to_silver) \
    #     .outputMode("append") \
    #     .trigger(processingTime="1 minutes") \
    #     .option("checkpointLocation", CHECKPOINT_LOCATION) \
    #     .queryName("silver_transformer") \
    #     .start()
    
    logger.info(f"✅ Silver transformer started! Checkpoint: {CHECKPOINT_LOCATION}")
    # query.awaitTermination()

if __name__ == "__main__":
    main()