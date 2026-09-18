import pytest

from src.jobs.train_als import build_als, evaluate_rmse, grid_search


def _synthetic_ratings(spark):
    """Hai cụm sở thích tách biệt để ALS học được cấu trúc.

    User 1-5 thích phim 1-5; user 6-10 thích phim 6-10. Cụm chéo bị chấm thấp.
    """
    rows = []
    for user in range(1, 6):
        for movie in range(1, 6):
            rows.append((user, movie, 5.0))
        for movie in range(6, 11):
            rows.append((user, movie, 1.0))
    for user in range(6, 11):
        for movie in range(1, 6):
            rows.append((user, movie, 1.0))
        for movie in range(6, 11):
            rows.append((user, movie, 5.0))
    return spark.createDataFrame(rows, "userId int, movieId int, rating double")


def test_build_als_sets_cold_start_drop(spark):
    als = build_als(rank=5, reg_param=0.1)
    # Thiếu tham số này thì RMSE ra NaN với mọi user/phim chưa thấy trong train
    assert als.getColdStartStrategy() == "drop"


def test_build_als_sets_checkpoint_interval(spark):
    als = build_als(rank=5, reg_param=0.1)
    assert als.getCheckpointInterval() > 0


def test_build_als_uses_movielens_column_names(spark):
    als = build_als(rank=5, reg_param=0.1)
    assert als.getUserCol() == "userId"
    assert als.getItemCol() == "movieId"
    assert als.getRatingCol() == "rating"


def test_evaluate_rmse_returns_finite_number(spark):
    data = _synthetic_ratings(spark)
    model = build_als(rank=5, reg_param=0.05, max_iter=5).fit(data)

    rmse = evaluate_rmse(model, data)

    assert rmse == pytest.approx(rmse)      # không phải NaN
    assert 0.0 <= rmse < 5.0


def test_als_beats_a_constant_predictor_on_learnable_data(spark):
    """Kiểm tra mô hình thực sự học được, không chỉ chạy không lỗi."""
    data = _synthetic_ratings(spark)
    model = build_als(rank=5, reg_param=0.01, max_iter=10).fit(data)

    rmse = evaluate_rmse(model, data)

    # Dự đoán hằng số tại trung bình (3.0) trên dữ liệu toàn 1.0/5.0 cho RMSE = 2.0
    assert rmse < 1.0


def test_grid_search_returns_one_row_per_combination(spark):
    data = _synthetic_ratings(spark)

    results = grid_search(data, data, ranks=[2, 4], reg_params=[0.1])

    assert len(results) == 2
    assert {r["rank"] for r in results} == {2, 4}
    for row in results:
        assert "rmse" in row and "fit_seconds" in row


def test_grid_search_honors_fixed_hyperparameters_from_env(spark, monkeypatch):
    """Task 11 cần đo thời gian của MỘT lần fit lặp lại được, không phải cả lưới.

    Khi cả ALS_RANK và ALS_REG_PARAM được đặt qua biến môi trường, grid_search
    phải bỏ qua toàn bộ lưới ranks/reg_params được truyền vào và chỉ fit đúng
    một tổ hợp cố định đó.
    """
    monkeypatch.setenv("ALS_RANK", "7")
    monkeypatch.setenv("ALS_REG_PARAM", "0.03")
    monkeypatch.setattr("src.jobs.train_als.config.ALS_FIXED_RANK", 7)
    monkeypatch.setattr("src.jobs.train_als.config.ALS_FIXED_REG_PARAM", 0.03)

    data = _synthetic_ratings(spark)

    results = grid_search(data, data, ranks=[2, 4, 8], reg_params=[0.01, 0.1])

    assert len(results) == 1
    assert results[0]["rank"] == 7
    assert results[0]["regParam"] == 0.03
