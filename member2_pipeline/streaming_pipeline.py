
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    date_format,
    when,
    coalesce,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


# ============================================================
# 1. PROJECT PATHS AND CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MEMBER2_ROOT = (
    PROJECT_ROOT
    / "member2_spark_streaming"
    / "member2_spark_streaming"
)

KAFKA_BOOTSTRAP = "localhost:9092"

TOPICS = (
    "ecommerce.browsing,"
    "ecommerce.orders,"
    "ecommerce.payments"
)

LANDING_PATH = MEMBER2_ROOT / "data" / "lake" / "validated_events"
DLQ_PATH = MEMBER2_ROOT / "data" / "lake" / "dlq"

CHECKPOINT_VALID = MEMBER2_ROOT / "checkpoints" / "valid_stream"
CHECKPOINT_DLQ = MEMBER2_ROOT / "checkpoints" / "dlq_stream"

# Create required directories
for folder in [
    LANDING_PATH,
    DLQ_PATH,
    CHECKPOINT_VALID,
    CHECKPOINT_DLQ
]:
    folder.mkdir(parents=True, exist_ok=True)

print("=" * 60)
print("MEMBER 2 - KAFKA SPARK STREAMING PIPELINE")
print("=" * 60)

print("Project root:", PROJECT_ROOT)
print("Member 2 root:", MEMBER2_ROOT)
print("Validated events:", LANDING_PATH)
print("DLQ:", DLQ_PATH)
print("Kafka broker:", KAFKA_BOOTSTRAP)
print("Kafka topics:", TOPICS)


# ============================================================
# 2. INITIALIZE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Member2-KafkaSparkStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "2"
    )
    .config(
        "spark.sql.streaming.forceDeleteTempCheckpointLocation",
        "true"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. EVENT SCHEMA
# ============================================================

event_schema = StructType([
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("search_query", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("refund_amount", DoubleType(), True)
])

VALID_EVENT_TYPES = [
    "search",
    "product_view",
    "add_to_cart",
    "purchase",
    "payment",
    "order_update"
]


# ============================================================
# 4. READ EVENTS FROM KAFKA
# ============================================================

print("\nConnecting to Kafka...")

raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPICS)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Use "latest" to process newly arriving events.
# Use "earliest" instead if you intentionally want to replay
# events already present in Kafka.


# ============================================================
# 5. PARSE KAFKA JSON EVENTS
# ============================================================

parsed_df = (
    raw_kafka_df
    .select(
        col("topic").alias("kafka_topic"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_payload")
    )
    .withColumn(
        "parsed",
        from_json(col("raw_payload"), event_schema)
    )
    .select(
        col("kafka_topic"),
        col("kafka_timestamp"),
        col("raw_payload"),

        col("parsed.schema_version").alias("schema_version"),
        col("parsed.event_id").alias("event_id"),
        col("parsed.event_type").alias("event_type"),

        to_timestamp(
            col("parsed.event_timestamp")
        ).alias("event_timestamp"),

        col("parsed.customer_id").alias("customer_id"),
        col("parsed.session_id").alias("session_id"),
        col("parsed.device").alias("device"),
        col("parsed.country").alias("country"),
        col("parsed.currency").alias("currency"),
        col("parsed.product_id").alias("product_id"),
        col("parsed.category").alias("category"),
        col("parsed.price").alias("price"),
        col("parsed.quantity").alias("quantity"),
        col("parsed.search_query").alias("search_query"),
        col("parsed.order_id").alias("order_id"),
        col("parsed.payment_method").alias("payment_method"),
        col("parsed.payment_status").alias("payment_status"),
        col("parsed.amount").alias("amount"),
        col("parsed.order_status").alias("order_status"),
        col("parsed.refund_amount").alias("refund_amount")
    )
)


# ============================================================
# 6. VALIDATE EVENTS
# ============================================================

validation_condition = (
    col("event_id").isNotNull()
    & col("customer_id").isNotNull()
    & col("session_id").isNotNull()
    & col("event_timestamp").isNotNull()
    & col("event_type").isin(VALID_EVENT_TYPES)
)

# Treat null validation results as invalid.
validation_condition = coalesce(
    validation_condition,
    lit(False)
)

# Route invalid events to DLQ
malformed_stream = (
    parsed_df
    .filter(~validation_condition)
    .withColumn(
        "dlq_reason",
        when(
            col("event_id").isNull(),
            "Missing or Null event_id"
        )
        .when(
            col("customer_id").isNull(),
            "Missing customer_id"
        )
        .when(
            col("session_id").isNull(),
            "Missing session_id"
        )
        .when(
            col("event_timestamp").isNull(),
            "Missing or unparseable timestamp"
        )
        .when(
            ~coalesce(
                col("event_type").isin(VALID_EVENT_TYPES),
                lit(False)
            ),
            "Invalid event_type"
        )
        .otherwise("Malformed JSON body")
    )
    .withColumn("ingestion_time", current_timestamp())
)

# Valid events
valid_stream = (
    parsed_df
    .filter(validation_condition)
    .withColumn(
        "event_date",
        date_format(col("event_timestamp"), "yyyy-MM-dd")
    )
    .withColumn(
        "ingestion_time",
        current_timestamp()
    )
)


# ============================================================
# 7. WATERMARK AND DEDUPLICATION
# ============================================================

deduplicated_stream = (
    valid_stream
    .withWatermark("event_timestamp", "10 minutes")
    .dropDuplicates(["event_id", "event_timestamp"])
)


# ============================================================
# 8. WRITE VALID EVENTS TO PARQUET
# ============================================================

print("\nStarting validated Parquet writer...")

valid_writer = (
    deduplicated_stream.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", str(LANDING_PATH))
    .option("checkpointLocation", str(CHECKPOINT_VALID))
    .partitionBy("event_date")
    .trigger(processingTime="10 seconds")
    .start()
)


# ============================================================
# 9. WRITE MALFORMED EVENTS TO DLQ
# ============================================================

print("Starting DLQ writer...")

dlq_writer = (
    malformed_stream.writeStream
    .format("json")
    .outputMode("append")
    .option("path", str(DLQ_PATH))
    .option("checkpointLocation", str(CHECKPOINT_DLQ))
    .trigger(processingTime="10 seconds")
    .start()
)


# ============================================================
# 10. DISPLAY STREAMING EVENTS IN CONSOLE
# ============================================================

console_writer = (
    deduplicated_stream
    .select(
        "event_id",
        "event_type",
        "event_timestamp",
        "customer_id",
        "price",
        "order_status"
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)


# ============================================================
# 11. PIPELINE STATUS
# ============================================================

print("\n" + "=" * 60)
print("SPARK STREAMING PIPELINE IS RUNNING")
print("=" * 60)

print("Kafka topics:", TOPICS)
print("Valid output:", LANDING_PATH)
print("DLQ output:", DLQ_PATH)
print("Checkpointing: Enabled")
print("Watermark: 10 minutes")
print("Processing interval: 10 seconds")
print("\nWaiting for Kafka events...")


# ============================================================
# 12. GRACEFUL SHUTDOWN
# ============================================================

try:
    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    print("\nStopping Spark Streaming Pipeline...")

finally:
    for query in spark.streams.active:
        query.stop()

    spark.stop()

    print("Spark Streaming Pipeline stopped.")
