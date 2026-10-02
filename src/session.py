"""Khởi tạo SparkSession dùng chung cho mọi job trong cụm Big Data."""
import os
from typing import Optional

from pyspark.sql import SparkSession

from src import config


def get_spark(app_name: str, master: Optional[str] = None) -> SparkSession:
    """Tạo SparkSession và thiết lập HDFS replication, parquet snappy và checkpoint dir."""
    master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "2g"))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    )
    if config.USE_HDFS:
        builder = builder.config("spark.hadoop.dfs.replication", str(config.HDFS_REPLICATION))
        builder = builder.config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE)

    spark = builder.getOrCreate()

    checkpoint_dir = f"{config.HDFS_NAMENODE}/checkpoint" if config.USE_HDFS else str(config.CHECKPOINT_DIR)
    if not config.USE_HDFS:
        config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    return spark
