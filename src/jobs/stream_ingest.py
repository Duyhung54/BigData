#!/usr/bin/env python
"""
Job: Đọc rating mới từ Kafka → ghi vào Delta Lake (incremental).
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct
from pyspark.sql.types import StructType, IntegerType, FloatType, LongType

from src import config

# Schema của rating (cùng với CSV)
RATING_SCHEMA = StructType() \
    .add("userId", IntegerType()) \
    .add("movieId", IntegerType()) \
    .add("rating", FloatType()) \
    .add("timestamp", LongType())

def get_spark(app_name: str = "stream_ingest") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Giới hạn số lượng batch checkpoint lưu lại (mặc định 100 -> giảm còn 5 để không phình ổ cứng)
        .config("spark.sql.streaming.minBatchesToRetain", "5")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )

def main() -> None:
    spark = get_spark()
    # Đọc từ Kafka (key/value đều là string)
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_RATINGS_TOPIC)
        .option("startingOffsets", "latest")   # hoặc "earliest" cho test
        .load()
    )

    # Convert value (bytes) → JSON → columns
    ratings = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
           .select(from_json(col("json_str"), RATING_SCHEMA).alias("data"))
           .select("data.*")
    )

    # Ghi vào Delta Lake, tự động partition theo year‑month nếu muốn
    query = (
        ratings.writeStream.format("delta")
               .outputMode("append")
               .option("checkpointLocation", str(config.CHECKPOINT_DIR / "stream_ingest"))
               .trigger(processingTime="10 seconds")   # micro‑batch mỗi 10s
               .start(str(config.DELTA_LAKE_PATH))
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()