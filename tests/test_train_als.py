import pytest

from src import config
from src.jobs.train_als import build_als, evaluate_rmse, grid_search, resolve_output_paths


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


def test_resolve_output_paths_uses_canonical_dirs_by_default(monkeypatch):
    """Grid search thật (không đặt ALS_RANK/ALS_REG_PARAM) phải ghi vào đúng
    nơi Job 3 và report/figures/tuning.png đọc: config.RESULTS_DIR/MODEL_DIR.
    """
    monkeypatch.setattr("src.jobs.train_als.config.ALS_FIXED_RANK", None)
    monkeypatch.setattr("src.jobs.train_als.config.ALS_FIXED_REG_PARAM", None)

    results_dir, model_dir, is_scaling_run = resolve_output_paths()

    assert results_dir == config.RESULTS_DIR
    assert model_dir == config.MODEL_DIR
    assert is_scaling_run is False


def test_resolve_output_paths_redirects_away_from_canonical_dirs_when_scaling(monkeypatch):
    """Task 11 (scripts/scaling_experiment.sh) đặt cả hai biến này để đo thời
    gian một tổ hợp cố định lặp lại 9 lần. Job KHÔNG được ghi đè
    config.RESULTS_DIR/tuning.csv (lưới tuning thật, 15 dòng) hay
    config.MODEL_DIR (mô hình đang phục vụ) trong chế độ này.
    """
    monkeypatch.setattr("src.jobs.train_als.config.ALS_FIXED_RANK", 10)
    monkeypatch.setattr("src.jobs.train_als.config.ALS_FIXED_REG_PARAM", 0.1)

    results_dir, model_dir, is_scaling_run = resolve_output_paths()

    assert is_scaling_run is True
    assert results_dir != config.RESULTS_DIR
    assert model_dir != config.MODEL_DIR
    assert results_dir == config.SCALING_RESULTS_DIR
    assert model_dir == config.SCALING_MODEL_DIR
    # Cả hai đường scratch phải nằm dưới thư mục kết quả/output đã cấu hình,
    # không phải một nhánh bịa ra tách biệt hoàn toàn.
    assert str(results_dir).startswith(str(config.RESULTS_DIR))
    assert str(model_dir).startswith(str(config.OUTPUT_DIR))
