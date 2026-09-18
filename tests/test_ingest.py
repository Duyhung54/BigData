from pyspark.sql import types as T

from src.common.schema import RATINGS_SCHEMA, MOVIES_SCHEMA
from src.jobs.ingest import ESTIMATED_PARQUET_RATIO, TARGET_FILE_MB, _n_output_files, read_ratings_csv


def test_ratings_schema_has_exact_types():
    assert RATINGS_SCHEMA == T.StructType([
        T.StructField("userId", T.IntegerType(), True),
        T.StructField("movieId", T.IntegerType(), True),
        T.StructField("rating", T.DoubleType(), True),
        T.StructField("timestamp", T.LongType(), True),
    ])


def test_movies_schema_has_exact_types():
    assert MOVIES_SCHEMA == T.StructType([
        T.StructField("movieId", T.IntegerType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
    ])


def test_read_ratings_csv_applies_schema_not_strings(spark, tmp_path):
    csv = tmp_path / "ratings.csv"
    csv.write_text("userId,movieId,rating,timestamp\n1,10,4.0,1000\n2,20,3.5,2000\n")
    df = read_ratings_csv(spark, str(csv))
    assert df.schema == RATINGS_SCHEMA
    assert df.count() == 2
    assert df.filter("userId = 1").first()["rating"] == 4.0


def test_n_output_files_never_zero_for_tiny_input():
    assert _n_output_files(0) == 1
    assert _n_output_files(1024) == 1


def test_n_output_files_at_ml25m_scale_targets_parquet_size_not_csv_size():
    # ratings.csv của ml-25m nặng khoảng 650 MB.
    csv_bytes = 650 * 1024 * 1024
    n = _n_output_files(csv_bytes)

    estimated_parquet_mb = csv_bytes * ESTIMATED_PARQUET_RATIO / (1024 * 1024)
    mb_per_file = estimated_parquet_mb / n

    # Chia theo dung lượng Parquet ước tính (~30% CSV), không phải dung lượng
    # CSV thô — nếu không mỗi file sẽ chỉ ~35 MB thay vì áp sát TARGET_FILE_MB.
    assert n == 2
    assert TARGET_FILE_MB / 2 <= mb_per_file <= TARGET_FILE_MB * 1.5
