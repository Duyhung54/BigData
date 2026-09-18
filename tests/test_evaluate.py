import pytest

from src.jobs.evaluate import ranking_metrics, rating_metrics, relevant_items


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
