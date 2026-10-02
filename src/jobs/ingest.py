"""Job 1: CSV -> Parquet, kèm đo thống kê cho báo cáo."""
import csv as csv_module
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src import config
from src.common.schema import (
    RATINGS_SCHEMA, 
    MOVIES_SCHEMA, 
    GENOME_SCORES_SCHEMA, 
    GENOME_TAGS_SCHEMA, 
    TAGS_SCHEMA, 
    LINKS_SCHEMA
)
from src.session import get_spark
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, collect_list, concat_ws

TARGET_FILE_MB = 128

ESTIMATED_PARQUET_RATIO = 0.30


def read_ratings_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=RATINGS_SCHEMA)


def read_movies_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=MOVIES_SCHEMA, escape='"')


def read_genome_scores_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=GENOME_SCORES_SCHEMA)


def read_genome_tags_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=GENOME_TAGS_SCHEMA)


def read_tags_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=TAGS_SCHEMA, escape='"')


def read_links_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=LINKS_SCHEMA)


def _dir_bytes(path) -> int:
    if isinstance(path, str) and path.startswith("hdfs://"):
        return 0  # HDFS bytes checking skipped for demo
    path = Path(str(path))
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _n_output_files(csv_bytes: int) -> int:
    """Gộp về các file Parquet ~128 MB.

    Chia dung lượng PARQUET ƯỚC TÍNH, không phải dung lượng CSV. Parquet+Snappy
    chỉ còn khoảng 30% so với CSV, nên chia thẳng csv_bytes sẽ cho ra file nhỏ
    hơn mục tiêu khoảng 3 lần — ở ml-25m là ~35 MB/file thay vì 128 MB.

    KHÔNG partition theo userId: 162.000 user sẽ sinh 162.000 thư mục con —
    lỗi small-files kinh điển. Mọi job phía sau đều đọc toàn bộ dữ liệu nên
    partition theo cột không đem lại lợi ích gì.
    """
    estimated_parquet_bytes = csv_bytes * ESTIMATED_PARQUET_RATIO
    return max(1, round(estimated_parquet_bytes / (TARGET_FILE_MB * 1024 * 1024)))


def ingest(spark: SparkSession) -> dict:
    csv_path = config.RATINGS_CSV
    csv_bytes = _dir_bytes(csv_path)

    # Khởi động Spark trước khi bấm giờ bất cứ thứ gì.
    # Không có bước này, phép đo đầu tiên (inferSchema) gánh luôn chi phí một lần
    # của JVM, cấp executor và sinh mã Catalyst — đo trên ml-latest-small cho
    # inferSchema 10,08s so với schema tường minh 0,35s, tức 29 lần, trong khi
    # chi phí thật của một lượt quét thêm chỉ khoảng 2 lần. Số liệu đó đi thẳng
    # vào báo cáo nên phải đo cho đúng.
    spark.range(1).count()

    # Khởi động thêm lần nữa, lần này CHẠM THẬT vào file CSV, trước khi bấm giờ.
    # spark.range(1).count() ở trên chỉ khởi động JVM/executor/Catalyst chung
    # chung — nó không đụng tới csv_path, nên lượt đọc có bấm giờ đầu tiên
    # (inferSchema) vẫn phải gánh riêng: liệt kê file, nạp trang OS cache, và
    # sinh mã codegen cho CSV datasource — những chi phí mà lượt đọc thứ hai
    # (explicit schema) không phải trả nữa. Đó là lý do tỷ lệ đo được ở trên
    # kẹt quanh 8 lần thay vì ~2 lần như dự đoán. Đọc bằng spark.read.text
    # (không phải inferSchema) để không tự thiên vị: nếu khởi động bằng đúng
    # phép đọc inferSchema sẽ làm khoảng cách đo được bị thu hẹp giả tạo. Đọc
    # text trung tính làm nóng phần chi phí CẢ HAI lượt đọc có bấm giờ đều
    # dùng chung (liệt kê file, OS cache), còn phần chênh lệch thật sự của
    # inferSchema (thêm một lượt quét kiểu dữ liệu) vẫn được đo đúng.
    spark.read.text(str(csv_path)).count()

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

    # Nạp các bảng mở rộng của MovieLens 25M
    if config.GENOME_SCORES_CSV.exists():
        print(f"Đang nạp genome-scores.csv (15.58M dòng) lên HDFS...")
        genome_scores = read_genome_scores_csv(spark, str(config.GENOME_SCORES_CSV))
        genome_scores.repartition(4).write.mode("overwrite").parquet(str(config.GENOME_SCORES_PARQUET))

    if config.GENOME_TAGS_CSV.exists():
        print(f"Đang nạp genome-tags.csv lên HDFS...")
        genome_tags = read_genome_tags_csv(spark, str(config.GENOME_TAGS_CSV))
        genome_tags.coalesce(1).write.mode("overwrite").parquet(str(config.GENOME_TAGS_PARQUET))

    if config.TAGS_CSV.exists():
        print(f"Đang nạp tags.csv (1.09M dòng) lên HDFS...")
        tags = read_tags_csv(spark, str(config.TAGS_CSV))
        tags.coalesce(2).write.mode("overwrite").parquet(str(config.TAGS_PARQUET))

    if config.LINKS_CSV.exists():
        print(f"Đang nạp links.csv lên HDFS...")
        links = read_links_csv(spark, str(config.LINKS_CSV))
        links.coalesce(1).write.mode("overwrite").parquet(str(config.LINKS_PARQUET))

    # Trích xuất Top-5 Tag đặc trưng của từng phim (dùng cho Explainable AI)
    if config.GENOME_SCORES_CSV.exists() and config.GENOME_TAGS_CSV.exists():
        print("Đang trích xuất Top-5 Tags đặc trưng cho từng phim...")
        w = Window.partitionBy("movieId").orderBy(col("relevance").desc())
        ranked = genome_scores.join(genome_tags, "tagId") \
            .withColumn("rn", row_number().over(w)) \
            .filter(col("rn") <= 5)
        top_tags = ranked.groupBy("movieId").agg(concat_ws(", ", collect_list("tag")).alias("top_tags"))
        top_tags.coalesce(1).write.mode("overwrite").parquet(str(config.MOVIE_TOP_TAGS_PARQUET))
        print("Đã lưu bảng movie_top_tags lên HDFS thành công!")

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
