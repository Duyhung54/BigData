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


def _write_tiny_movielens(raw_dir):
    """6 rating + 2 phim, một tựa phim có dấu phẩy để kiểm tra escape."""
    raw_dir.mkdir(parents=True)
    (raw_dir / "ratings.csv").write_text(
        "userId,movieId,rating,timestamp\n"
        "1,10,4.0,100\n1,20,3.5,200\n2,10,5.0,150\n"
        "2,20,2.0,250\n3,10,1.0,300\n3,20,4.5,350\n",
        encoding="utf-8",
    )
    (raw_dir / "movies.csv").write_text(
        "movieId,title,genres\n"
        '10,"American President, The (1995)",Comedy|Drama|Romance\n'
        "20,Toy Story (1995),Adventure|Animation\n",
        encoding="utf-8",
    )


def test_ingest_writes_lake_under_lake_root(spark, tmp_path, monkeypatch):
    """Chạy ingest() từ đầu đến cuối, lake trỏ vào thư mục tạm qua LAKE_ROOT.

    Trước test này không có gì gọi ingest() — nên đổi đường dẫn lake từ Path
    sang chuỗi (để trỏ được sang hdfs://) có thể làm _dir_bytes/mkdir hỏng ở
    runtime mà suite vẫn xanh. Không ghi gì vào đường dẫn thật: mọi đường dẫn
    trong config đều được monkeypatch vào tmp_path.
    """
    from src import config
    from src.jobs import ingest as ingest_job

    raw = tmp_path / "raw"
    _write_tiny_movielens(raw)
    lake = str(tmp_path / "lake")

    monkeypatch.setattr(config, "RATINGS_CSV", raw / "ratings.csv")
    monkeypatch.setattr(config, "MOVIES_CSV", raw / "movies.csv")
    monkeypatch.setattr(config, "LAKE_ROOT", lake, raising=False)
    monkeypatch.setattr(config, "RATINGS_PARQUET", lake + "/ratings.parquet")
    monkeypatch.setattr(config, "MOVIES_PARQUET", lake + "/movies.parquet")

    stats = ingest_job.ingest(spark)

    assert stats["n_ratings"] == 6
    assert stats["n_output_files"] == 1
    assert stats["parquet_bytes"] > 0
    assert spark.read.parquet(lake + "/ratings.parquet").count() == 6

    titles = {r["title"] for r in spark.read.parquet(lake + "/movies.parquet").collect()}
    assert "American President, The (1995)" in titles


def test_path_bytes_counts_parquet_directory(spark, tmp_path):
    """_path_bytes đo qua Hadoop FS nên đúng cả với chuỗi hdfs:// lẫn local.

    Suite không có cụm HDFS, nên kiểm tra trên đường dẫn local — Hadoop FS xử
    lý local qua scheme file://, cùng một đoạn code chạy ở cả hai nơi.
    """
    from src.jobs import ingest as ingest_job

    out = tmp_path / "t.parquet"
    spark.range(1000).write.mode("overwrite").parquet(str(out))

    size = ingest_job._path_bytes(spark, str(out))

    # Không khẳng định bằng tuyệt đối: Spark ghi kèm _SUCCESS và các file .crc
    # mà hai cách đếm có thể tính khác nhau.
    actual = ingest_job._dir_bytes(out)
    assert size > 0
    assert 0.5 * actual <= size <= 1.5 * actual


def test_path_bytes_handles_single_file(spark, tmp_path):
    """Đúng cả khi đường dẫn là MỘT file — _dir_bytes cũ có nhánh riêng cho việc này."""
    from src.jobs import ingest as ingest_job

    f = tmp_path / "x.txt"
    f.write_text("0123456789", encoding="utf-8")

    assert ingest_job._path_bytes(spark, str(f)) == 10
