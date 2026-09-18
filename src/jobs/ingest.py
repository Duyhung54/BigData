"""Job 1: CSV -> Parquet, kèm đo thống kê cho báo cáo."""
import csv as csv_module
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src import config
from src.common.schema import RATINGS_SCHEMA, MOVIES_SCHEMA
from src.session import get_spark

TARGET_FILE_MB = 128


def read_ratings_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=RATINGS_SCHEMA)


def read_movies_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=MOVIES_SCHEMA, escape='"')


def _dir_bytes(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _n_output_files(n_bytes: int) -> int:
    """Gộp về các file ~128 MB.

    KHÔNG partition theo userId: 162.000 user sẽ sinh 162.000 thư mục con —
    lỗi small-files kinh điển. Mọi job phía sau đều đọc toàn bộ dữ liệu nên
    partition theo cột không đem lại lợi ích gì.
    """
    return max(1, round(n_bytes / (TARGET_FILE_MB * 1024 * 1024)))


def ingest(spark: SparkSession) -> dict:
    csv_path = config.RATINGS_CSV
    csv_bytes = _dir_bytes(csv_path)

    # Đo thời gian khi dùng inferSchema, để so sánh trong báo cáo
    t0 = time.perf_counter()
    spark.read.csv(str(csv_path), header=True, inferSchema=True).count()
    infer_schema_seconds = time.perf_counter() - t0

    t0 = time.perf_counter()
    ratings = read_ratings_csv(spark, str(csv_path))
    n_ratings = ratings.count()
    csv_read_seconds = time.perf_counter() - t0

    config.LAKE_DIR.mkdir(parents=True, exist_ok=True)
    n_files = _n_output_files(csv_bytes)
    ratings.repartition(n_files).write.mode("overwrite").parquet(str(config.RATINGS_PARQUET))

    movies = read_movies_csv(spark, str(config.MOVIES_CSV))
    movies.coalesce(1).write.mode("overwrite").parquet(str(config.MOVIES_PARQUET))

    t0 = time.perf_counter()
    spark.read.parquet(str(config.RATINGS_PARQUET)).count()
    parquet_read_seconds = time.perf_counter() - t0

    return {
        "dataset": config.DATASET,
        "n_ratings": n_ratings,
        "csv_bytes": csv_bytes,
        "parquet_bytes": _dir_bytes(config.RATINGS_PARQUET),
        "infer_schema_seconds": round(infer_schema_seconds, 3),
        "csv_read_seconds": round(csv_read_seconds, 3),
        "parquet_read_seconds": round(parquet_read_seconds, 3),
        "n_output_files": n_files,
    }


def main() -> None:
    spark = get_spark("ingest")
    stats = ingest(spark)
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.RESULTS_DIR / "ingest_stats.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=list(stats))
        writer.writeheader()
        writer.writerow(stats)
    print(f"Ghi thống kê ingest: {out}")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    spark.stop()


if __name__ == "__main__":
    main()
