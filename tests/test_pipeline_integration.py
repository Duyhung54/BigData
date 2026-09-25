"""Chạy trọn pipeline trên dữ liệu tổng hợp nhỏ, kiểm tra các job nối được."""
import sqlite3

import numpy as np
import pytest

from src.common.split import add_split_column
from src.jobs.evaluate import ranking_metrics, relevant_items
from src.jobs.export_recs import top_rated_per_user, write_sqlite
from src.jobs.train_als import build_als, evaluate_rmse
from pyspark.sql import functions as F


@pytest.fixture
def ratings(spark):
    """40 user x 20 phim, hai cụm sở thích, timestamp tăng dần.

    Thứ tự chấm phim được xoay theo user (`order`) thay vì dùng thẳng `movie`
    làm mốc thời gian. Nếu dùng thẳng `movie`, mọi user chấm phim theo đúng
    một thứ tự (1..20) nên add_split_column (chia theo percentile thời gian
    TRONG TỪNG USER) luôn đưa cùng 3 movieId cuối (18,19,20) vào tập test cho
    MỌI user — ba phim đó không bao giờ xuất hiện trong train, khiến toàn bộ
    dự đoán trên test bị coldStartStrategy="drop" loại sạch, và
    RegressionEvaluator ném IllegalArgumentException trên Spark 3.5 thay vì
    trả NaN. Đây là lỗi của cách sinh dữ liệu tổng hợp, không phải lỗi của
    add_split_column hay ALS — dữ liệu thật không có kiểu tương quan thứ tự
    tuyệt đối này giữa mọi user. Xoay thứ tự theo user để mỗi phim rơi vào
    tail của một số user chứ không phải tất cả, giống dữ liệu thật hơn.
    """
    rows = []
    for user in range(1, 41):
        cluster = 0 if user <= 20 else 1
        for movie in range(1, 21):
            liked = (movie <= 10) == (cluster == 0)
            order = ((movie - 1 + user) % 20) + 1
            rows.append((user, movie, 5.0 if liked else 1.5, 1_000 + user * 100 + order))
    return spark.createDataFrame(
        rows, "userId int, movieId int, rating double, timestamp long"
    )


def test_pipeline_produces_usable_recommendations(spark, ratings, tmp_path):
    # --- Job 2 tương đương: chia tập ---
    split = add_split_column(ratings)
    train = split.filter("split IN ('train','val')").select("userId", "movieId", "rating")
    test = split.filter("split = 'test'").select("userId", "movieId", "rating")
    assert train.count() > 0 and test.count() > 0

    # --- Job 2: huấn luyện ---
    model = build_als(rank=8, reg_param=0.05, max_iter=10).fit(train)
    rmse = evaluate_rmse(model, test)
    assert not np.isnan(rmse), "RMSE là NaN — coldStartStrategy chưa được đặt"
    assert rmse < 2.0

    # --- Job 3: đánh giá ---
    actual = relevant_items(test, threshold=4.0)
    recs = model.recommendForUserSubset(actual.select("userId"), 5).select(
        "userId", F.col("recommendations.movieId").alias("items")
    )
    metrics = ranking_metrics(recs, actual, k=5)
    assert 0.0 <= metrics["ndcg_at_k"] <= 1.0

    # --- Job 4: xuất sang SQLite ---
    recs_pdf = (
        model.recommendForAllUsers(5)
        .select("userId", F.posexplode("recommendations").alias("pos", "rec"))
        .select(
            "userId",
            F.col("rec.movieId").alias("movieId"),
            (F.col("pos") + 1).alias("rank"),
            F.col("rec.rating").cast("double").alias("score"),
        )
        .toPandas()
    )
    movies_pdf = (
        ratings.select("movieId").distinct()
        .withColumn("title", F.concat(F.lit("Phim "), F.col("movieId")))
        .withColumn("genres", F.lit("Drama"))
        .toPandas()
    )
    sample_pdf = top_rated_per_user(ratings, n=5).toPandas()

    db = tmp_path / "recs.sqlite"
    write_sqlite(recs_pdf, movies_pdf, sample_pdf, db)

    # --- Kiểm tra tầng serving đọc được ---
    with sqlite3.connect(db) as conn:
        n_recs = conn.execute(
            "SELECT COUNT(*) FROM recommendations WHERE userId = 1"
        ).fetchone()[0]
        n_history = conn.execute(
            "SELECT COUNT(*) FROM ratings_sample WHERE userId = 1"
        ).fetchone()[0]
    assert n_recs == 5
    assert n_history == 5
