"""Job 3: Spark Structured Streaming - Luồng Stream phục vụ Real-time Trending và Delta Lake.
Quy tắc:
1. Đọc sự kiện tương tác từ Kafka topic 'user_events'.
2. Append sự kiện thô vào Delta Lake trên HDFS để phục vụ tích lũy dữ liệu.
3. Tính toán Sliding Window (1 giờ trượt 10 phút) đếm tần suất xem phim.
4. Xuất Top 20 phim thịnh hành vào Redis Sorted Set key 'trending:movies:1h' kèm TTL 600s.
5. TUYỆT ĐỐI KHÔNG huấn luyện lại ALS trong luồng này.
"""
import os
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

from src import config

# Schema sự kiện tương tác gửi qua Kafka
USER_EVENT_SCHEMA = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])


def write_to_redis_batch(batch_df, batch_id):
    """ForeachBatch: Lấy Top 20 phim của cửa sổ gần nhất và ghi vào Redis Sorted Set."""
    try:
        if batch_df.isEmpty():
            return

        r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=0)

        # Lấy Top 20 phim có lượt tương tác cao nhất
        top20 = (
            batch_df
            .orderBy(F.col("count").desc())
            .limit(20)
            .collect()
        )

        if not top20:
            return

        # Chuẩn bị mapping {movieId: count} cho Redis ZADD
        zadd_mapping = {str(row["movieId"]): float(row["count"]) for row in top20}

        pipe = r.pipeline()
        redis_key = "trending:movies:1h"
        # Xóa key cũ và nạp lại top mới
        pipe.delete(redis_key)
        pipe.zadd(redis_key, zadd_mapping)
        # Thiết lập TTL 600s (10 phút)
        pipe.expire(redis_key, 600)
        pipe.execute()

        print(f"[STREAM][Batch {batch_id}] Đã cập nhật Top {len(top20)} Trending vào Redis (key: {redis_key})")
    except Exception as e:
        print(f"[STREAM][Batch {batch_id}] Lỗi khi ghi vào Redis: {e}")


def main() -> None:
    print(f"=== KHỞI ĐỘNG SPARK STRUCTURED STREAMING ===")
    print(f"Kafka Broker: {config.KAFKA_BOOTSTRAP_SERVERS} | Topic: {config.KAFKA_TOPIC}")
    print(f"Redis Host: {config.REDIS_HOST}:{config.REDIS_PORT}")
    print(f"Delta Lake Path: {config.DELTA_LAKE_PATH}")

    builder = (
        SparkSession.builder
        .appName("movielens-stream-trending")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.minBatchesToRetain", "5")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    if config.USE_HDFS:
        builder = builder.config("spark.hadoop.dfs.replication", str(config.HDFS_REPLICATION))
        builder = builder.config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc stream từ Kafka
    raw_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2. Giải mã JSON và trích xuất trường dữ liệu
    parsed_stream = (
        raw_kafka.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), USER_EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    # Chuyển đổi timestamp Unix sang TimestampType để áp dụng Windowing
    stream_with_event_time = parsed_stream.withColumn(
        "event_time",
        F.to_timestamp(F.from_unixtime(F.col("timestamp")))
    )

    # 3. Luồng ghi Delta Lake (Append Raw Data)
    checkpoint_delta = str(config.CHECKPOINT_DIR / "delta_append") if not config.USE_HDFS else f"{config.HDFS_NAMENODE}/checkpoint/delta_append"
    delta_query = (
        parsed_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_delta)
        .trigger(processingTime="10 seconds")
        .start(str(config.DELTA_LAKE_PATH))
    )

    # 4. Luồng tính Sliding Window (1 hour, slide 10 mins) -> Redis
    windowed_trending = (
        stream_with_event_time
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window(F.col("event_time"), "1 hour", "10 minutes"),
            F.col("movieId")
        )
        .count()
    )

    checkpoint_redis = str(config.CHECKPOINT_DIR / "redis_trending") if not config.USE_HDFS else f"{config.HDFS_NAMENODE}/checkpoint/redis_trending"
    redis_query = (
        windowed_trending.writeStream
        .outputMode("complete")
        .foreachBatch(write_to_redis_batch)
        .option("checkpointLocation", checkpoint_redis)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("-> Luồng Streaming đang chạy ngầm... Đợi nhận sự kiện.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

