import importlib

from src.session import get_spark
from src import config


def test_spark_session_counts_rows(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], "n int")
    assert df.count() == 3


def test_config_paths_are_under_data_root():
    assert config.RATINGS_PARQUET.is_relative_to(config.DATA_ROOT)
    assert config.MODEL_DIR.is_relative_to(config.DATA_ROOT)


def test_get_spark_sets_checkpoint_dir(spark, tmp_path, monkeypatch):
    """Nhận fixture `spark` để session dùng chung được tạo TRƯỚC.

    getOrCreate trả về session đang tồn tại, nên nếu test này chạy đầu tiên nó
    sẽ khoá cả JVM vào master="local[1]" cho mọi test sau.
    """
    monkeypatch.setattr(config, "CHECKPOINT_DIR", tmp_path / "ckpt")

    session = get_spark("smoke")

    assert session.sparkContext.getCheckpointDir() is not None
    # không stop: fixture `spark` sở hữu vòng đời của session này


def test_als_fixed_params_default_to_none_when_env_unset(monkeypatch):
    """Task 11 dùng ALS_RANK/ALS_REG_PARAM để ép fit một tổ hợp duy nhất.

    Khi hai biến môi trường này không được đặt, config phải trả về None để
    train_als.py biết là chạy grid search bình thường.
    """
    monkeypatch.delenv("ALS_RANK", raising=False)
    monkeypatch.delenv("ALS_REG_PARAM", raising=False)
    importlib.reload(config)

    try:
        assert config.ALS_FIXED_RANK is None
        assert config.ALS_FIXED_REG_PARAM is None
    finally:
        importlib.reload(config)
