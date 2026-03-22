from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, when, trim, lower,
    datediff, to_date, lit, sha2, coalesce
)
from pyspark.sql.types import BooleanType
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCS_BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
GCS_SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/checkpoints/silver")

def create_spark_session():
    spark = SparkSession.builder \
        .appName("BronzeToSilverTransformer") \
        .config("spark.jars.packages", 
                "com.google.cloud.spark:spark-3.5_2.12:0.35.0") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
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
    
    output_path = f"gs://{GCS_SILVER_BUCKET}/cleaned/users/"
    
    valid_df.write \
        .format("parquet") \
        .mode("append") \
        .partitionBy("event_date") \
        .option("compression", "snappy") \
        .save(output_path)
    
    # Write invalid records to quarantine
    invalid_df = df.filter(col("is_complete") == False)
    if invalid_df.count() > 0:
        quarantine_path = f"gs://{GCS_SILVER_BUCKET}/quarantine/users/"
        invalid_df.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("event_date") \
            .save(quarantine_path)
        logger.info(f"{invalid_df.count()} records sent to quarantine")
    
    logger.info(f"Batch {batch_id} written to Silver layer")

def main():
    logger.info("Starting Silver Layer Transformer...")
    
    spark = create_spark_session()
    
    bronze_df = spark.readStream \
        .format("parquet") \
        .option("maxFilesPerTrigger", 10) \
        .load(f"gs://{GCS_BRONZE_BUCKET}/raw/users/")
    
    silver_df = validate_and_clean(bronze_df)
    
    query = silver_df.writeStream \
        .foreachBatch(write_to_silver) \
        .outputMode("append") \
        .trigger(processingTime="5 minutes") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .queryName("silver_transformer") \
        .start()
    
    logger.info(f"✅ Silver transformer started! Checkpoint: {CHECKPOINT_LOCATION}")
    query.awaitTermination()

if __name__ == "__main__":
    main()