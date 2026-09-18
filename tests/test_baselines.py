import pytest

from src.common.baselines import (
    global_mean_predictions,
    item_mean_predictions,
    popularity_top_n,
)

SCHEMA = "userId int, movieId int, rating double"


def test_global_mean_predicts_same_value_everywhere(spark):
    train = spark.createDataFrame([(1, 10, 2.0), (2, 20, 4.0)], SCHEMA)
    target = spark.createDataFrame([(3, 30, 5.0), (4, 40, 1.0)], SCHEMA)

    result = global_mean_predictions(train, target).collect()

    assert {round(r["prediction"], 6) for r in result} == {3.0}
    assert len(result) == 2


def test_item_mean_uses_per_movie_average(spark):
    train = spark.createDataFrame(
        [(1, 10, 2.0), (2, 10, 4.0), (3, 20, 5.0)], SCHEMA
    )
    target = spark.createDataFrame([(9, 10, 0.0), (9, 20, 0.0)], SCHEMA)

    preds = {r["movieId"]: r["prediction"] for r in item_mean_predictions(train, target).collect()}

    assert preds[10] == pytest.approx(3.0)   # (2+4)/2
    assert preds[20] == pytest.approx(5.0)


def test_item_mean_falls_back_to_global_mean_for_unseen_movie(spark):
    train = spark.createDataFrame([(1, 10, 2.0), (2, 10, 4.0)], SCHEMA)
    target = spark.createDataFrame([(9, 999, 0.0)], SCHEMA)

    prediction = item_mean_predictions(train, target).first()["prediction"]

    assert prediction == pytest.approx(3.0)  # trung bình toàn cục


def test_popularity_ranks_by_rating_count_not_average(spark):
    # phim 10 có 3 lượt điểm thấp, phim 20 có 1 lượt điểm cao.
    # Popularity phải xếp phim 10 trước.
    train = spark.createDataFrame(
        [(1, 10, 1.0), (2, 10, 1.0), (3, 10, 1.0), (4, 20, 5.0)], SCHEMA
    )

    assert popularity_top_n(train, n=2) == [10, 20]


def test_popularity_respects_n(spark):
    train = spark.createDataFrame(
        [(1, 10, 4.0), (2, 10, 4.0), (3, 20, 4.0)], SCHEMA
    )

    assert popularity_top_n(train, n=1) == [10]
