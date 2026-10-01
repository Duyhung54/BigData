"""LAKE_ROOT phải là chuỗi, không phải Path.

Path("hdfs://namenode:9000/x") cho ra "hdfs:/namenode:9000/x" — mất một dấu
gạch chéo và Spark không phân giải được URI. Đây là lỗi âm thầm: không có
exception nào, chỉ là Spark báo không tìm thấy đường dẫn.
"""
import importlib

from src import config


def test_lake_root_defaults_to_data_root_lake(monkeypatch):
    monkeypatch.delenv("LAKE_ROOT", raising=False)
    importlib.reload(config)
    try:
        assert config.LAKE_ROOT == str(config.DATA_ROOT / "lake")
        assert config.RATINGS_PARQUET.endswith("/ratings.parquet")
        assert config.MOVIES_PARQUET.endswith("/movies.parquet")
    finally:
        importlib.reload(config)


def test_lake_root_env_keeps_uri_intact(monkeypatch):
    """Khẳng định URI còn NGUYÊN hai dấu gạch chéo sau "hdfs:".

    Nếu ai đó đổi LAKE_ROOT về Path, test này fail ngay với
    "hdfs:/namenode..." thay vì "hdfs://namenode...".
    """
    monkeypatch.setenv("LAKE_ROOT", "hdfs://namenode:9000/movielens/lake")
    importlib.reload(config)
    try:
        assert config.LAKE_ROOT == "hdfs://namenode:9000/movielens/lake"
        assert config.RATINGS_PARQUET == "hdfs://namenode:9000/movielens/lake/ratings.parquet"
        assert config.MOVIES_PARQUET == "hdfs://namenode:9000/movielens/lake/movies.parquet"
    finally:
        importlib.reload(config)


def test_is_remote_lake_false_for_local_path(monkeypatch):
    monkeypatch.delenv("LAKE_ROOT", raising=False)
    importlib.reload(config)
    try:
        assert config.is_remote_lake() is False
    finally:
        importlib.reload(config)


def test_is_remote_lake_true_for_hdfs(monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", "hdfs://namenode:9000/movielens/lake")
    importlib.reload(config)
    try:
        assert config.is_remote_lake() is True
    finally:
        importlib.reload(config)


def test_is_remote_lake_false_for_file_scheme(monkeypatch):
    """file:// là URI nhưng vẫn là hệ thống file cục bộ.

    Coi nó là "xa" sẽ làm ingest bỏ qua mkdir và lần ghi đầu tiên vào một thư
    mục chưa tồn tại sẽ hỏng.
    """
    monkeypatch.setenv("LAKE_ROOT", "file:///opt/data/lake")
    importlib.reload(config)
    try:
        assert config.is_remote_lake() is False
    finally:
        importlib.reload(config)
