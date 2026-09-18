"""Khởi tạo SparkSession dùng chung cho mọi job."""
import os
from typing import Optional

from pyspark.sql import SparkSession

from src import config


def get_spark(app_name: str, master: Optional[str] = None) -> SparkSession:
    """Tạo SparkSession và đặt checkpoint dir.

    Checkpoint dir bắt buộc phải có: ALS lặp nhiều vòng sinh lineage RDD rất
    dài và sẽ ném StackOverflowError nếu không được cắt định kỳ.
    """
    master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "2g"))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(config.CHECKPOINT_DIR))
    return spark
