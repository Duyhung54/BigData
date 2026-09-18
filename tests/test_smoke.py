import importlib

from src.session import get_spark
from src import config


def test_spark_session_counts_rows(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], "n int")
    assert df.count() == 3


def test_config_paths_are_under_data_root():
    # Không dùng Path.is_relative_to: nó chỉ có từ Python 3.9, container Spark chạy 3.8
    root = str(config.DATA_ROOT)
    assert str(config.RATINGS_PARQUET).startswith(root)
    assert str(config.MODEL_DIR).startswith(root)


def test_get_spark_sets_checkpoint_dir(spark, tmp_path, monkeypatch):
    """Nhận fixture `spark` để session dùng chung được tạo TRƯỚC.

    getOrCreate trả về session đang tồn tại, nên nếu test này chạy đầu tiên nó
    sẽ khoá cả JVM vào master="local[1]" cho mọi test sau.
    """
    checkpoint_dir = tmp_path / "ckpt"
    monkeypatch.setattr(config, "CHECKPOINT_DIR", checkpoint_dir)

    session = get_spark("smoke")

    actual = session.sparkContext.getCheckpointDir()
    assert actual is not None
    # getCheckpointDir() có thể trả về file:// URI và đổi dấu phân cách, nên so
    # sánh theo chuỗi con đã chuẩn hoá thay vì so bằng tuyệt đối.
    assert str(checkpoint_dir).replace("\\", "/") in actual.replace("\\", "/")
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
