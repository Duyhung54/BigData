"""_apply_hdfs_config: thiết lập phía CLIENT HDFS cho Spark khi lake ở HDFS.

Không test qua get_spark(): getOrCreate() trả về session đang tồn tại nên mọi
assertion về config sẽ pass bất kể code đúng hay sai (lỗi tautological đã
từng xảy ra trong dự án với test_get_spark_sets_checkpoint_dir). Builder giả
bên dưới ghi lại đúng những lời gọi .config() thật sự diễn ra.
"""
import importlib

from src import config, session


class _FakeBuilder:
    """Ghi lại các lời gọi .config(key, value) và trả về chính mình như Builder thật."""

    def __init__(self):
        self.calls = []

    def config(self, key, value):
        self.calls.append((key, value))
        return self

    def keys(self):
        return [key for key, _ in self.calls]


def _with_lake_root(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("LAKE_ROOT", raising=False)
    else:
        monkeypatch.setenv("LAKE_ROOT", value)
    importlib.reload(config)


def test_sets_client_replication_and_blocksize_when_lake_on_hdfs(monkeypatch):
    """Replication của một file do CLIENT quyết định lúc ghi, mặc định là 3.

    Cụm chỉ có 2 DataNode, nên không đặt lại thì mọi block Spark ghi ra đều
    bị NameNode đánh dấu under-replicated.
    """
    _with_lake_root(monkeypatch, "hdfs://namenode:9000/movielens/lake")
    try:
        builder = _FakeBuilder()
        result = session._apply_hdfs_config(builder)

        assert result is builder
        assert ("spark.hadoop.dfs.replication", str(config.HDFS_REPLICATION)) in builder.calls
        assert ("spark.hadoop.dfs.blocksize", str(config.HDFS_BLOCKSIZE_BYTES)) in builder.calls
    finally:
        importlib.reload(config)


def test_never_sets_default_fs(monkeypatch):
    """KHÔNG BAO GIỜ đổi fs.defaultFS, kể cả khi lake ở HDFS.

    Đổi defaultFS làm mọi đường dẫn không có scheme ("/opt/data/raw/...",
    "/opt/data/output/model", checkpoint...) bị hiểu là đường dẫn TRÊN HDFS:
    ingest không tìm thấy CSV, shutil.copytree trong eligibility.py không thấy
    model. Lake được trỏ tới bằng URI đầy đủ trong LAKE_ROOT nên không cần nó.
    """
    _with_lake_root(monkeypatch, "hdfs://namenode:9000/movielens/lake")
    try:
        builder = _FakeBuilder()
        session._apply_hdfs_config(builder)

        assert not any("defaultFS" in key for key in builder.keys())
    finally:
        importlib.reload(config)


def test_is_noop_when_lake_is_local(monkeypatch):
    """Lake ở local thì KHÔNG gọi .config() lần nào.

    Đây là điều kiện để pipeline bind mount chạy y hệt như trước và số liệu
    ml-25m đã đo không phải chạy lại.
    """
    _with_lake_root(monkeypatch, None)
    try:
        builder = _FakeBuilder()
        session._apply_hdfs_config(builder)

        assert builder.calls == []
    finally:
        importlib.reload(config)


def test_hdfs_constants_match_cluster_config():
    """Hai con số phía client phải khớp docker/hadoop/hdfs-site.xml.

    Lệch nhau thì hoặc block bị under-replicated (client xin nhiều replica hơn
    số DataNode), hoặc file Parquet 128 MB bị cắt qua hai block.
    """
    assert config.HDFS_REPLICATION == 2
    assert config.HDFS_BLOCKSIZE_BYTES == 128 * 1024 * 1024
