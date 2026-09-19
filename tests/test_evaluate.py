import pytest
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from src.jobs.evaluate import (
    eligible_items,
    ranking_metrics,
    rating_metrics,
    relevant_items,
    restrict_model_to_eligible_items,
)


def test_relevant_items_keeps_only_ratings_at_or_above_threshold(spark):
    test = spark.createDataFrame(
        [(1, 10, 5.0), (1, 11, 4.0), (1, 12, 3.5), (1, 13, 2.0)],
        "userId int, movieId int, rating double",
    )

    row = relevant_items(test, threshold=4.0).first()

    assert set(row["relevant"]) == {10, 11}


def test_relevant_items_drops_users_with_no_relevant_ratings(spark):
    test = spark.createDataFrame(
        [(1, 10, 5.0), (2, 20, 1.0)],
        "userId int, movieId int, rating double",
    )

    users = {r["userId"] for r in relevant_items(test, threshold=4.0).collect()}

    assert users == {1}


def test_ranking_metrics_averages_over_users(spark):
    # user 1: gợi ý [1,2], liên quan {1}    -> precision@2 = 0.5
    # user 2: gợi ý [3,4], liên quan {3,4}  -> precision@2 = 1.0
    # trung bình = 0.75
    recs = spark.createDataFrame(
        [(1, [1, 2]), (2, [3, 4])], "userId int, items array<int>"
    )
    actual = spark.createDataFrame(
        [(1, [1]), (2, [3, 4])], "userId int, relevant array<int>"
    )

    result = ranking_metrics(recs, actual, k=2)

    assert result["precision_at_k"] == pytest.approx(0.75)
    assert result["recall_at_k"] == pytest.approx(1.0)


def test_ranking_metrics_ignores_users_without_ground_truth(spark):
    recs = spark.createDataFrame(
        [(1, [1, 2]), (99, [7, 8])], "userId int, items array<int>"
    )
    actual = spark.createDataFrame([(1, [1])], "userId int, relevant array<int>")

    result = ranking_metrics(recs, actual, k=2)

    assert result["precision_at_k"] == pytest.approx(0.5)


def test_rating_metrics_computes_rmse_and_mae(spark):
    # sai số: +1, -1 -> RMSE = 1.0, MAE = 1.0
    predictions = spark.createDataFrame(
        [(3.0, 4.0), (5.0, 4.0)], "rating double, prediction double"
    )

    result = rating_metrics(predictions)

    assert result["rmse"] == pytest.approx(1.0)
    assert result["mae"] == pytest.approx(1.0)


def test_eligible_items_keeps_only_movies_at_or_above_min_ratings(spark):
    # movie 10: 2 lượt đánh giá, movie 11: 1 lượt -> chỉ 10 đủ điều kiện với min_ratings=2
    train_val = spark.createDataFrame(
        [(1, 10, 5.0), (2, 10, 4.0), (1, 11, 3.0)],
        "userId int, movieId int, rating double",
    )

    result = {r["movieId"] for r in eligible_items(train_val, min_ratings=2).collect()}

    assert result == {10}


def test_restrict_model_to_eligible_items_limits_recommendations_to_eligible_ids(spark, tmp_path):
    # movie 12 không đủ điều kiện (lẽ ra ALS vẫn có thể gợi ý nó nếu không lọc);
    # sau khi lọc, mọi gợi ý phải nằm trong {10, 11}.
    ratings = spark.createDataFrame(
        [
            (1, 10, 5.0), (1, 11, 4.0), (1, 12, 1.0),
            (2, 10, 4.0), (2, 11, 5.0), (2, 12, 2.0),
            (3, 10, 3.0), (3, 11, 3.0), (3, 12, 5.0),
        ],
        "userId int, movieId int, rating double",
    )
    model = ALS(
        userCol="userId", itemCol="movieId", ratingCol="rating",
        rank=2, maxIter=5, seed=42, coldStartStrategy="drop",
    ).fit(ratings)
    eligible = spark.createDataFrame([(10,), (11,)], "movieId int")

    restricted = restrict_model_to_eligible_items(
        spark, model, eligible, tmp_path / "model_eligible"
    )

    factor_ids = {r["id"] for r in restricted.itemFactors.collect()}
    assert factor_ids == {10, 11}

    test_users = spark.createDataFrame([(1,), (2,), (3,)], "userId int")
    recs = (
        restricted.recommendForUserSubset(test_users, 2)
        .select("userId", F.col("recommendations.movieId").alias("items"))
        .collect()
    )
    for row in recs:
        assert set(row["items"]) <= {10, 11}
        assert 12 not in row["items"]
