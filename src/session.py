"""Khởi tạo SparkSession dùng chung cho mọi job."""
import os
from typing import Optional

from pyspark.sql import SparkSession

from src import config


def _apply_hdfs_config(builder):
    """Thiết lập phía CLIENT HDFS khi lake nằm trên HDFS.

    Replication và blocksize của một file do CLIENT quyết định lúc ghi, không
    phải NameNode. Không đặt lại thì Spark xin mặc định 3 replica trong khi cụm
    chỉ có 2 DataNode — mọi block bị NameNode đánh dấu under-replicated.

    KHÔNG BAO GIỜ đổi fs.defaultFS: đổi nó làm mọi đường dẫn không có scheme
    ("/opt/data/raw/...", "/opt/data/output/model", checkpoint...) bị hiểu là
    đường dẫn TRÊN HDFS — ingest không tìm thấy CSV, shutil.copytree trong
    eligibility.py không thấy model. Lake được trỏ tới bằng URI đầy đủ trong
    config.LAKE_ROOT ("hdfs://namenode:9000/..."), Hadoop client đọc địa chỉ
    NameNode ngay từ URI đó nên không cần defaultFS.

    Lake ở local thì không gọi .config() lần nào, để hành vi giống hệt trước
    khi có HDFS. Tách thành hàm riêng để test được bằng builder giả —
    getOrCreate() trả về session đang tồn tại nên test qua get_spark() sẽ
    tautological.
    """
    if config.is_remote_lake():
        builder = (
            builder
            .config("spark.hadoop.dfs.replication", str(config.HDFS_REPLICATION))
            .config("spark.hadoop.dfs.blocksize", str(config.HDFS_BLOCKSIZE_BYTES))
        )
    return builder


def get_spark(app_name: str, master: Optional[str] = None) -> SparkSession:
    """Tạo SparkSession và đặt checkpoint dir.

    Checkpoint dir bắt buộc phải có: ALS lặp nhiều vòng sinh lineage RDD rất
    dài và sẽ ném StackOverflowError nếu không được cắt định kỳ.
    """
    master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "2g"))
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Mặc định Spark là false: checkpoint của ALS bị bỏ lại trên đĩa mãi mãi
        # thay vì bị dọn khi RDD tham chiếu tới nó hết vòng đời. Vô hại ở quy mô
        # ml-latest-small, nhưng chạy cả lưới 15 tổ hợp (Task 6) trên ml-25m thì
        # tích tụ hàng chục GB checkpoint mồ côi trong CHECKPOINT_DIR.
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    )
    spark = _apply_hdfs_config(builder).getOrCreate()
    config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(config.CHECKPOINT_DIR))
    return spark
