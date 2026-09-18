import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """SparkSession local dùng chung cho toàn bộ test.

    shuffle.partitions = 2 vì dữ liệu test rất nhỏ; để mặc định 200 sẽ khiến
    mỗi test mất vài giây chỉ để tạo partition rỗng.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setCheckpointDir(str(tmp_path_factory.mktemp("ckpt")))
    yield session
    session.stop()
