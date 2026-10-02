"""Job 1: ETL Ingest - Chuyển toàn bộ 6 file CSV của MovieLens sang Parquet Snappy trên HDFS.
Quy tắc:
1. Đọc đúng 6 file từ thư mục raw/ml-32m.
2. Áp dụng schema tường minh để tránh inferSchema tốn I/O.
3. KHÔNG DÙNG repartition: Giữ nguyên phân vùng tự nhiên của Spark để tránh Small Files Problem.
4. Ghi trực tiếp ra định dạng Apache Parquet nén Snappy trên HDFS.
"""
import time
from pathlib import Path
from pyspark.sql import SparkSession

from src import config
from src.common.schema import (
    RATINGS_SCHEMA,
    MOVIES_SCHEMA,
    TAGS_SCHEMA,
    LINKS_SCHEMA,
    GENOME_SCORES_SCHEMA,
    GENOME_TAGS_SCHEMA,
)
from src.session import get_spark


def ingest(spark: SparkSession) -> dict:
    print(f"=== BẮT ĐẦU INGEST 6 TỆP MOVIELENS ({config.DATASET}) LÊN HDFS ===")
    start_total = time.perf_counter()

    files_to_process = [
        ("ratings", config.RATINGS_CSV, config.RATINGS_PARQUET, RATINGS_SCHEMA),
        ("movies", config.MOVIES_CSV, config.MOVIES_PARQUET, MOVIES_SCHEMA),
        ("tags", config.TAGS_CSV, config.TAGS_PARQUET, TAGS_SCHEMA),
        ("links", config.LINKS_CSV, config.LINKS_PARQUET, LINKS_SCHEMA),
        ("genome-scores", config.GENOME_SCORES_CSV, config.GENOME_SCORES_PARQUET, GENOME_SCORES_SCHEMA),
        ("genome-tags", config.GENOME_TAGS_CSV, config.GENOME_TAGS_PARQUET, GENOME_TAGS_SCHEMA),
    ]

    stats = {}
    completed_files = 0

    for name, csv_path, parquet_path, schema in files_to_process:
        print(f"\n--- Đang xử lý: {name} ---")
        csv_file = Path(csv_path)

        if not csv_file.exists():
            print(f"[CẢNH BÁO] Không tìm thấy tệp {csv_file}. Bỏ qua...")
            continue

        t0 = time.perf_counter()
        # Đọc CSV với schema tường minh từ đĩa cục bộ của container
        file_uri = f"file://{csv_file.as_posix()}" if not str(csv_file).startswith("file://") else str(csv_file)
        df = spark.read.csv(file_uri, header=True, schema=schema, escape='"')
        row_count = df.count()
        read_duration = time.perf_counter() - t0
        print(f"-> Đọc xong: {row_count:,} dòng ({read_duration:.2f}s)")

        # Ghi thẳng Parquet Snappy, TUYỆT ĐỐI KHÔNG dùng repartition
        t1 = time.perf_counter()
        df.write.mode("overwrite").parquet(str(parquet_path))
        write_duration = time.perf_counter() - t1
        print(f"-> Ghi Parquet vào HDFS: {parquet_path} ({write_duration:.2f}s)")

        completed_files += 1
        stats[name] = {
            "rows": row_count,
            "read_time": round(read_duration, 2),
            "write_time": round(write_duration, 2),
            "destination": str(parquet_path)
        }

    total_time = time.perf_counter() - start_total
    print(f"\n==================================================")
    print(f"Ghi xong {completed_files}/6 file vào HDFS trong {total_time:.2f}s")
    print(f"Đích đến HDFS: {config.HDFS_PATH}")
    print(f"==================================================")

    return stats


def main() -> None:
    spark = get_spark("movielens-ingest-etl")
    try:
        ingest(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
