from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_SERVICE_ACCOUNT_PATH = os.getenv("GCP_SERVICE_ACCOUNT_PATH", "/opt/spark/config/gcp-service-account.json")

GOLD_TOPICS = {
    "daily_country_stats": {
        "topic": "GOLD_DAILY_COUNTRY_STATS",
        "table": "daily_country_stats",
        "dataset": "gold"
    },
    "hourly_stats": {
        "topic": "GOLD_HOURLY_STATS",
        "table": "hourly_stats",
        "dataset": "gold"
    }
}

DAILY_STATS_SCHEMA = StructType([
    StructField("COUNTRY_CODE", StringType(), True),
    StructField("TOTAL_USERS", LongType(), True),
    StructField("UNIQUE_USERS", LongType(), True),
    StructField("AVG_AGE", DoubleType(), True),
    StructField("MIN_AGE", IntegerType(), True),
    StructField("MAX_AGE", IntegerType(), True),
    StructField("EVENT_DATE", StringType(), True),
    StructField("LAST_UPDATED", LongType(), True) 
])

HOURLY_STATS_SCHEMA = StructType([
    StructField("COUNTRY_CODE", StringType(), True),
    StructField("USERS_THIS_HOUR", LongType(), True),
    StructField("UNIQUE_USERS_THIS_HOUR", LongType(), True),
    StructField("AVG_AGE_THIS_HOUR", DoubleType(), True),
    StructField("HOUR_BUCKET", StringType(), True),
    StructField("LAST_UPDATED", LongType(), True)
])

def create_spark_session():
    logger.info(f"🔐 GCP Project: {GCP_PROJECT_ID}")
    
    bq_connector = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0"
    kafka_connector = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    
    spark = SparkSession.builder \
        .appName("GoldToBigQuery") \
        .config("spark.jars.packages", f"{bq_connector},{kafka_connector}") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    if GCP_SERVICE_ACCOUNT_PATH and os.path.exists(GCP_SERVICE_ACCOUNT_PATH):
        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", GCP_SERVICE_ACCOUNT_PATH)
    
    logger.info("✅ Spark Session created")
    return spark

def write_gold_to_bigquery(df, batch_id, topic_config):
    if df.rdd.isEmpty():
        logger.info(f"📭 Batch {batch_id} is empty for {topic_config['table']}")
        return
    
    record_count = df.count()
    logger.info(f"📦 Processing batch {batch_id} with {record_count} records for {topic_config['table']}")
    
    logger.info("📄 Sample data:")
    df.limit(3).show(truncate=False)
    
    full_table_name = f"{GCP_PROJECT_ID}.{topic_config['dataset']}.{topic_config['table']}"
    
    try:
        df.write \
            .format("bigquery") \
            .option("table", full_table_name) \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        logger.info(f"✅ Batch {batch_id} written to BigQuery: {full_table_name}")
    except Exception as e:
        logger.error(f"❌ Failed to write to BigQuery: {e}")

def process_gold_topic(spark, topic_name, topic_config, schema):
    logger.info(f"🔗 Reading from Kafka topic: {topic_config['topic']}")
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic_config['topic']) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    final_df = parsed_df \
        .withColumn("last_updated_timestamp", (col("LAST_UPDATED") / 1000).cast(TimestampType())) \
        .withColumn("ingested_at", current_timestamp()) \
        .withColumn("source", lit("ksqldb-gold")) \
        .drop("LAST_UPDATED") 
    
    query = final_df.writeStream \
        .foreachBatch(lambda df, batch_id: write_gold_to_bigquery(df, batch_id, topic_config)) \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", f"/tmp/checkpoints/gold_{topic_name}") \
        .queryName(f"gold_{topic_name}_to_bq") \
        .start()
    
    logger.info(f"✅ Started streaming for {topic_name}")
    return query

def main():
    logger.info("=" * 60)
    logger.info("🚀 Starting Gold Layer: ksqlDB → BigQuery")
    logger.info("=" * 60)
    
    if not GCP_PROJECT_ID:
        logger.error("❌ GCP_PROJECT_ID not set! Please check your .env file.")
        return
    
    spark = create_spark_session()
    queries = []
    
    for topic_name, topic_config in GOLD_TOPICS.items():
        schema = DAILY_STATS_SCHEMA if "daily" in topic_name else HOURLY_STATS_SCHEMA
        query = process_gold_topic(spark, topic_name, topic_config, schema)
        queries.append(query)
    
    logger.info(f"✅ Started {len(queries)} Gold streaming queries")
    logger.info("=" * 60)
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()