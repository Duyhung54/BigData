# Hệ thống gợi ý MovieLens với Spark ALS — Kế hoạch triển khai

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xây dựng hệ thống gợi ý phim end-to-end trên MovieLens 25M: pipeline Spark 4 job chạy trên Docker cluster, mô hình ALS có tuning và đánh giá kèm baseline, tầng serving FastAPI tách rời, cùng thí nghiệm đo khả năng mở rộng.

**Architecture:** Hai tầng tách rời qua hệ thống file. Tầng batch gồm 4 job Spark độc lập (`ingest` → `train_als` → `evaluate` → `export_recs`), mỗi job đọc input từ đĩa và ghi output ra đĩa nên chạy lại được riêng lẻ. Tầng serving là FastAPI đọc SQLite và một mảng numpy nạp sẵn, **không bao giờ** khởi tạo SparkSession.

**Tech Stack:** Spark 3.5.3 (Docker Official Image `spark`), PySpark, Spark MLlib ALS, Parquet, SQLite, FastAPI, numpy, pandas, matplotlib, pytest, Docker Compose.

**Spec:** [docs/superpowers/specs/2026-09-18-movielens-als-recsys-design.md](../specs/2026-09-18-movielens-als-recsys-design.md)

## Global Constraints

Mọi task đều ngầm chịu các ràng buộc sau. Giá trị chép nguyên văn từ spec:

- **Image Spark:** Docker Official Image `spark:3.5.3-python3`. **Không dùng `bitnami/spark`** (Bitnami đã dời image miễn phí sang namespace `bitnamilegacy` trong 2025).
- **Phiên bản Spark:** 3.5.x, không dùng 4.x.
- **Mount dữ liệu:** mọi container mount `./data` vào **cùng một đường dẫn `/opt/data`**. Lệch path giữa driver và executor gây `FileNotFoundException` rất khó truy vết.
- **Cấu hình cluster mặc định:** 2 worker × 3 core × 3 GB; driver 2 GB.
- **`coldStartStrategy="drop"`** bắt buộc trên mọi ALS. Thiếu nó thì RMSE ra `NaN`.
- **Checkpoint bắt buộc:** `setCheckpointDir` + `checkpointInterval=5` trên ALS, chống `StackOverflowError` do lineage dài.
- **Chia tập:** theo thời gian trong từng user, 70% train / 15% validation / 15% test. **Không dùng `randomSplit`.** User có dưới **5** rating: toàn bộ vào train, loại khỏi đánh giá.
- **Ngưỡng liên quan:** rating **≥ 4.0** là relevant. K mặc định = **10**. Số gợi ý xuất ra mỗi user = **20**.
- **Tập test chỉ được chấm điểm đúng một lần**, sau khi siêu tham số đã chốt trên tập validation.
- **Tầng serving không được import pyspark.**
- **Frontend không có bước build** — không npm, không bundler.
- **numpy phải < 2.0** (numpy 2.x phá vỡ interop giữa PySpark 3.5 và pandas).
- **Script `.sh` phải có line ending LF**, kể cả khi làm việc trên Windows.
- **Container Spark chạy Python 3.8.10** (đã kiểm chứng: `docker run --rm --entrypoint python3 spark:3.5.3-python3 --version`). Mọi code chạy trong container đó — `src/`, `tests/`, `report/` — phải tương thích Python 3.8:
  - **Không dùng cú pháp PEP 604** `X | None`. Dùng `typing.Optional[X]`. Trên 3.8 nó ném `TypeError` ngay lúc định nghĩa hàm, tức là job chết lúc import chứ không phải lúc chạy.
  - **Không dùng generic builtin trong annotation** (`list[int]`, `dict[str, float]`). Dùng `typing.List[int]`, `typing.Dict[str, float]`, hoặc bare `list` / `dict`.
  - **Không dùng `Path.is_relative_to()`** (Python 3.9+). Dùng so sánh chuỗi hoặc `os.path.commonpath`.
  - **matplotlib phải `>=3.7,<3.8`** — matplotlib 3.8 yêu cầu Python 3.9+, không có wheel cp38.
  - Lý do không nâng Python: PySpark bắt buộc phiên bản Python của driver và executor phải khớp nhau. Cài Python mới vào image Spark làm tăng rủi ro lệch phiên bản, không đáng đổi cho một đồ án có deadline.
  - Ngoại lệ: `serving/` chạy trên image `python:3.11-slim` riêng, nhưng vẫn viết theo chuẩn 3.8 để test của nó chạy được trong container Spark qua `scripts/test.sh`.

---

## Cấu trúc file

| File | Trách nhiệm |
|---|---|
| `docker/Dockerfile.spark` | Image Spark + thư viện Python cho tầng batch |
| `docker/Dockerfile.app` | Image Python thuần cho FastAPI (không có Java, không có Spark) |
| `docker/docker-compose.yml` | Định nghĩa master, worker, app; mount `/opt/data` |
| `src/config.py` | Tập trung mọi đường dẫn và hằng số. Không hardcode ở nơi khác. |
| `src/session.py` | Tạo SparkSession, đặt checkpoint dir |
| `src/common/schema.py` | Schema tường minh cho `ratings.csv`, `movies.csv` |
| `src/common/metrics.py` | Precision/Recall/NDCG/Coverage — **Python thuần, không import pyspark** |
| `src/common/split.py` | Chia tập theo thời gian trong từng user |
| `src/common/baselines.py` | Ba baseline: global mean, item mean, popularity |
| `src/jobs/ingest.py` | CSV → Parquet, đo thống kê |
| `src/jobs/train_als.py` | Grid search trên tập validation |
| `src/jobs/evaluate.py` | Chấm tập test, so sánh baseline |
| `src/jobs/export_recs.py` | Xuất recs ra SQLite + item factors ra .npy |
| `serving/api.py` | FastAPI |
| `serving/static/index.html` | Giao diện demo |
| `report/make_figures.py` | Sinh biểu đồ từ các file CSV kết quả |

`metrics.py` cố ý không phụ thuộc Spark: đây là phần logic dễ cài sai nhất mà vẫn cho ra số trông hợp lý, nên nó phải test được trong một giây thay vì phải dựng cluster.

---

## Task 1: Khung dự án và Spark cluster chạy được

**Files:**
- Create: `requirements-spark.txt`, `requirements-app.txt`, `.gitattributes`
- Create: `docker/Dockerfile.spark`, `docker/Dockerfile.app`, `docker/docker-compose.yml`
- Create: `src/__init__.py`, `src/config.py`, `src/session.py`
- Create: `src/common/__init__.py`, `src/jobs/__init__.py`
- Create: `scripts/download_data.sh`, `scripts/test.sh`
- Create: `tests/__init__.py`, `tests/conftest.py`, `tests/test_smoke.py`
- Create: `README.md`

**Interfaces:**
- Consumes: không có (task đầu tiên)
- Produces:
  - `src.config` — các hằng: `DATA_ROOT: Path`, `RAW_DIR: Path`, `LAKE_DIR: Path`, `OUTPUT_DIR: Path`, `CHECKPOINT_DIR: Path`, `RESULTS_DIR: Path`, `DATASET: str`, `RELEVANCE_THRESHOLD: float = 4.0`, `TOP_K: int = 10`, `N_RECOMMENDATIONS: int = 20`, `SPLIT_TRAIN: float = 0.70`, `SPLIT_VAL: float = 0.85`, `MIN_RATINGS_PER_USER: int = 5`
  - `src.session.get_spark(app_name: str, master: Optional[str] = None) -> SparkSession`

- [ ] **Step 1: Tạo `.gitattributes` chặn CRLF cho script**

```
* text=auto eol=lf
*.sh text eol=lf
*.png binary
*.npy binary
*.parquet binary
*.sqlite binary
```

Không có file này, Git trên Windows sẽ đổi `.sh` sang CRLF và container Linux báo `exec format error` hoặc `bad interpreter` — lỗi trông rất khó hiểu.

- [ ] **Step 2: Tạo hai file requirements**

`requirements-spark.txt` (PySpark đã có sẵn trong image, không cài lại):

```
numpy>=1.24,<2.0
pandas>=2.0,<3.0
pyarrow>=14.0
matplotlib>=3.7,<3.8
pytest>=8.0
```

`requirements-app.txt`:

```
fastapi>=0.110
uvicorn[standard]>=0.29
numpy>=1.24,<2.0
pandas>=2.0,<3.0
pyarrow>=14.0
httpx>=0.27
pytest>=8.0
```

- [ ] **Step 3: Tạo `docker/Dockerfile.spark`**

```dockerfile
FROM spark:3.5.3-python3

USER root

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

ENV PYTHONPATH=/opt/app:$PYTHONPATH
WORKDIR /opt/app

USER spark
```

`PYTHONPATH=/opt/app` để `import src.config` hoạt động từ mọi job.

- [ ] **Step 4: Tạo `docker/Dockerfile.app`**

```dockerfile
FROM python:3.11-slim

COPY requirements-app.txt /tmp/requirements-app.txt
RUN pip install --no-cache-dir -r /tmp/requirements-app.txt

ENV PYTHONPATH=/opt/app
WORKDIR /opt/app

CMD ["uvicorn", "serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
```

Image này cố ý **không có Java và không có Spark** — nó là rào chắn vật lý cho ràng buộc "serving không đụng Spark": nếu ai đó lỡ `import pyspark` trong `api.py`, container sẽ chết ngay chứ không âm thầm chạy chậm.

- [ ] **Step 5: Tạo `docker/docker-compose.yml`**

```yaml
services:
  spark-master:
    build:
      context: ..
      dockerfile: docker/Dockerfile.spark
    image: movielens-spark:latest
    container_name: spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    environment:
      SPARK_MASTER_URL: spark://spark-master:7077
      PYTHONPATH: /opt/app
    ports:
      - "8080:8080"   # Master UI
      - "7077:7077"   # Master RPC
      - "4040:4040"   # Driver UI khi chạy spark-submit
    volumes:
      - ../data:/opt/data
      - ../src:/opt/app/src
      - ../tests:/opt/app/tests
      - ../report:/opt/app/report

  spark-worker:
    build:
      context: ..
      dockerfile: docker/Dockerfile.spark
    image: movielens-spark:latest
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      SPARK_WORKER_CORES: "${SPARK_WORKER_CORES:-3}"
      SPARK_WORKER_MEMORY: "${SPARK_WORKER_MEMORY:-3g}"
      PYTHONPATH: /opt/app
    deploy:
      replicas: ${SPARK_WORKER_REPLICAS:-2}
    volumes:
      - ../data:/opt/data
      - ../src:/opt/app/src
      - ../report:/opt/app/report

  app:
    build:
      context: ..
      dockerfile: docker/Dockerfile.app
    container_name: movielens-api
    depends_on:
      - spark-master
    ports:
      - "8000:8000"
    volumes:
      - ../data:/opt/data
      - ../serving:/opt/app/serving
```

`SPARK_WORKER_REPLICAS` và `SPARK_WORKER_CORES` để ngoài biến môi trường vì Task 11 sẽ đổi chúng để chạy thí nghiệm scale.

- [ ] **Step 6: Tạo `src/config.py`**

```python
"""Tập trung mọi đường dẫn và hằng số cấu hình.

Không hardcode đường dẫn hay ngưỡng ở bất kỳ file nào khác.
"""
import os
from pathlib import Path

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/data"))
RAW_DIR = DATA_ROOT / "raw"
LAKE_DIR = DATA_ROOT / "lake"
OUTPUT_DIR = DATA_ROOT / "output"
CHECKPOINT_DIR = DATA_ROOT / "checkpoint"
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/opt/app/report/results"))

# Bộ dữ liệu đang dùng: "ml-25m" cho chạy thật, "ml-latest-small" khi phát triển
DATASET = os.environ.get("DATASET", "ml-25m")

RATINGS_CSV = RAW_DIR / DATASET / "ratings.csv"
MOVIES_CSV = RAW_DIR / DATASET / "movies.csv"
RATINGS_PARQUET = LAKE_DIR / "ratings.parquet"
MOVIES_PARQUET = LAKE_DIR / "movies.parquet"

MODEL_DIR = OUTPUT_DIR / "model"
RECS_PARQUET = OUTPUT_DIR / "recommendations.parquet"
RECS_SQLITE = OUTPUT_DIR / "recs.sqlite"
ITEM_FACTORS_NPY = OUTPUT_DIR / "item_factors.npy"
ITEM_INDEX_PARQUET = OUTPUT_DIR / "item_index.parquet"

# Chia tập theo thời gian trong từng user
SPLIT_TRAIN = 0.70
SPLIT_VAL = 0.85
MIN_RATINGS_PER_USER = 5

# Đánh giá
RELEVANCE_THRESHOLD = 4.0
TOP_K = 10
N_RECOMMENDATIONS = 20

# Chỉ sinh gợi ý từ các phim có ít nhất ngần này lượt đánh giá trong tập huấn luyện.
# Không có ngưỡng này, ALS gợi ý toàn phim có TRUNG VỊ 1 lượt đánh giá: factor của
# chúng ước lượng từ đúng một quan sát nên điểm dự đoán bị đẩy lên cực trị và chiếm
# hết top-10, khiến NDCG@10 rớt xuống 0.0003 so với 0.031 của baseline popularity.
MIN_RATINGS_FOR_RECOMMENDATION = 20

# Lưới siêu tham số
ALS_RANKS = [10, 50, 100]
# regParam kéo tới 0.5 vì lần chạy thử trên ml-latest-small cho 0.2 thắng —
# mà 0.2 là giá trị lớn nhất được thử, tức tối ưu nằm ở BIÊN của lưới và chưa
# kết luận được. Thêm 0.3 và 0.5 để tối ưu nằm hẳn bên trong lưới.
ALS_REG_PARAMS = [0.01, 0.1, 0.2, 0.3, 0.5]
ALS_MAX_ITER = 10
ALS_CHECKPOINT_INTERVAL = 5
```

- [ ] **Step 7: Tạo `src/session.py`**

```python
"""Khởi tạo SparkSession dùng chung cho mọi job."""
import os
from typing import Optional

from pyspark.sql import SparkSession

from src import config


def get_spark(app_name: str, master: Optional[str] = None) -> SparkSession:
    """Tạo SparkSession và đặt checkpoint dir.

    Checkpoint dir bắt buộc phải có: ALS lặp nhiều vòng sinh lineage RDD rất
    dài và sẽ ném StackOverflowError nếu không được cắt định kỳ.
    """
    master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "2g"))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(config.CHECKPOINT_DIR))
    return spark
```

- [ ] **Step 8: Tạo `scripts/download_data.sh`**

```bash
#!/usr/bin/env bash
# Tải bộ dữ liệu MovieLens vào data/raw/
# Dùng: ./scripts/download_data.sh [ml-25m|ml-latest-small]
set -euo pipefail

DATASET="${1:-ml-25m}"
RAW_DIR="$(dirname "$0")/../data/raw"
mkdir -p "$RAW_DIR"

if [ -d "$RAW_DIR/$DATASET" ]; then
  echo "Đã có $RAW_DIR/$DATASET, bỏ qua."
  exit 0
fi

URL="https://files.grouplens.org/datasets/movielens/${DATASET}.zip"
echo "Đang tải $URL ..."
curl -fL "$URL" -o "$RAW_DIR/${DATASET}.zip"
unzip -q "$RAW_DIR/${DATASET}.zip" -d "$RAW_DIR"
rm "$RAW_DIR/${DATASET}.zip"
echo "Xong: $RAW_DIR/$DATASET"
```

- [ ] **Step 9: Tạo `scripts/test.sh`**

```bash
#!/usr/bin/env bash
# Chạy toàn bộ test bên trong container spark-master.
set -euo pipefail
cd "$(dirname "$0")/../docker"
docker compose exec -T spark-master python -m pytest /opt/app/tests -v "$@"
```

- [ ] **Step 10: Tạo `tests/conftest.py`**

```python
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
```

- [ ] **Step 11: Viết smoke test `tests/test_smoke.py`**

```python
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
    monkeypatch.setattr(config, "CHECKPOINT_DIR", tmp_path / "ckpt")

    session = get_spark("smoke")

    assert session.sparkContext.getCheckpointDir() is not None
    # không stop: fixture `spark` sở hữu vòng đời của session này
```

- [ ] **Step 12: Dựng cluster và chạy test**

```bash
./scripts/download_data.sh ml-latest-small
cd docker && docker compose up -d --build && cd ..
docker compose -f docker/docker-compose.yml ps
./scripts/test.sh
```

Expected: 3 test PASS. Mở http://localhost:8080 thấy Spark Master UI với 2 worker ALIVE.

- [ ] **Step 13: Viết `README.md`**

Phải có các mục: yêu cầu hệ thống; **bước chỉnh `%USERPROFILE%\.wslconfig`** (`[wsl2]` / `memory=12GB` rồi `wsl --shutdown`) kèm giải thích rằng thiếu bước này executor sẽ bị OOM-kill giữa chừng; lệnh tải dữ liệu; lệnh dựng cluster; lệnh chạy pipeline; lệnh chạy test; các URL (Spark UI 8080, API 8000).

- [ ] **Step 14: Commit**

```bash
git add .gitattributes requirements-*.txt docker/ src/ scripts/ tests/ README.md
git commit -m "feat: khung dự án và Spark cluster trên Docker"
```

---

## Task 2: Schema tường minh và job ingest

**Files:**
- Create: `src/common/schema.py`, `src/jobs/ingest.py`
- Test: `tests/test_ingest.py`

**Interfaces:**
- Consumes: `src.config`, `src.session.get_spark`
- Produces:
  - `src.common.schema.RATINGS_SCHEMA: StructType`, `MOVIES_SCHEMA: StructType`
  - `src.jobs.ingest.read_ratings_csv(spark, path) -> DataFrame` (cột: `userId int, movieId int, rating double, timestamp long`)
  - `src.jobs.ingest.ingest(spark) -> dict` trả về thống kê với các khoá: `csv_bytes`, `parquet_bytes`, `csv_read_seconds`, `parquet_read_seconds`, `infer_schema_seconds`, `n_ratings`

- [ ] **Step 1: Viết test thất bại `tests/test_ingest.py`**

```python
from pyspark.sql import types as T

from src.common.schema import RATINGS_SCHEMA, MOVIES_SCHEMA
from src.jobs.ingest import read_ratings_csv


def test_ratings_schema_has_exact_types():
    assert RATINGS_SCHEMA == T.StructType([
        T.StructField("userId", T.IntegerType(), True),
        T.StructField("movieId", T.IntegerType(), True),
        T.StructField("rating", T.DoubleType(), True),
        T.StructField("timestamp", T.LongType(), True),
    ])


def test_movies_schema_has_exact_types():
    assert MOVIES_SCHEMA == T.StructType([
        T.StructField("movieId", T.IntegerType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
    ])


def test_read_ratings_csv_applies_schema_not_strings(spark, tmp_path):
    csv = tmp_path / "ratings.csv"
    csv.write_text("userId,movieId,rating,timestamp\n1,10,4.0,1000\n2,20,3.5,2000\n")
    df = read_ratings_csv(spark, str(csv))
    assert df.schema == RATINGS_SCHEMA
    assert df.count() == 2
    assert df.filter("userId = 1").first()["rating"] == 4.0
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_ingest.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.schema'`

- [ ] **Step 3: Viết `src/common/schema.py`**

```python
"""Schema tường minh cho các file CSV của MovieLens.

Khai báo tường minh thay vì inferSchema=True: inferSchema buộc Spark quét
toàn bộ file thêm một lượt chỉ để suy ra kiểu dữ liệu — với ratings.csv 650 MB
đó là một lượt đọc thừa hoàn toàn tránh được.
"""
from pyspark.sql import types as T

RATINGS_SCHEMA = T.StructType([
    T.StructField("userId", T.IntegerType(), True),
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("timestamp", T.LongType(), True),
])

MOVIES_SCHEMA = T.StructType([
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("genres", T.StringType(), True),
])
```

- [ ] **Step 4: Viết `src/jobs/ingest.py`**

```python
"""Job 1: CSV -> Parquet, kèm đo thống kê cho báo cáo."""
import csv as csv_module
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src import config
from src.common.schema import RATINGS_SCHEMA, MOVIES_SCHEMA
from src.session import get_spark

TARGET_FILE_MB = 128

# Tỷ lệ nén CSV -> Parquet+Snappy, đo thực tế trên ml-latest-small: 670883/2483723 = 0.27.
# Dùng 0.30 cho an toàn. Cần hằng số này vì số file phải quyết định TRƯỚC khi ghi,
# mà lúc đó chưa biết dung lượng Parquet thật.
ESTIMATED_PARQUET_RATIO = 0.30


def read_ratings_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=RATINGS_SCHEMA)


def read_movies_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=MOVIES_SCHEMA, escape='"')


def _dir_bytes(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _n_output_files(csv_bytes: int) -> int:
    """Gộp về các file Parquet ~128 MB.

    Chia dung lượng PARQUET ƯỚC TÍNH, không phải dung lượng CSV. Parquet+Snappy
    chỉ còn khoảng 30% so với CSV, nên chia thẳng csv_bytes sẽ cho ra file nhỏ
    hơn mục tiêu khoảng 3 lần — ở ml-25m là ~35 MB/file thay vì 128 MB.

    KHÔNG partition theo userId: 162.000 user sẽ sinh 162.000 thư mục con —
    lỗi small-files kinh điển. Mọi job phía sau đều đọc toàn bộ dữ liệu nên
    partition theo cột không đem lại lợi ích gì.
    """
    estimated_parquet_bytes = csv_bytes * ESTIMATED_PARQUET_RATIO
    return max(1, round(estimated_parquet_bytes / (TARGET_FILE_MB * 1024 * 1024)))


def ingest(spark: SparkSession) -> dict:
    csv_path = config.RATINGS_CSV
    csv_bytes = _dir_bytes(csv_path)

    # Khởi động Spark trước khi bấm giờ bất cứ thứ gì.
    # Không có bước này, phép đo đầu tiên (inferSchema) gánh luôn chi phí một lần
    # của JVM, cấp executor và sinh mã Catalyst — đo trên ml-latest-small cho
    # inferSchema 10,08s so với schema tường minh 0,35s, tức 29 lần, trong khi
    # chi phí thật của một lượt quét thêm chỉ khoảng 2 lần. Số liệu đó đi thẳng
    # vào báo cáo nên phải đo cho đúng.
    spark.range(1).count()

    # Đo thời gian khi dùng inferSchema, để so sánh trong báo cáo
    t0 = time.perf_counter()
    spark.read.csv(str(csv_path), header=True, inferSchema=True).count()
    infer_schema_seconds = time.perf_counter() - t0

    t0 = time.perf_counter()
    ratings = read_ratings_csv(spark, str(csv_path))
    n_ratings = ratings.count()
    csv_read_seconds = time.perf_counter() - t0

    config.LAKE_DIR.mkdir(parents=True, exist_ok=True)
    n_files = _n_output_files(csv_bytes)
    ratings.repartition(n_files).write.mode("overwrite").parquet(str(config.RATINGS_PARQUET))

    movies = read_movies_csv(spark, str(config.MOVIES_CSV))
    movies.coalesce(1).write.mode("overwrite").parquet(str(config.MOVIES_PARQUET))

    t0 = time.perf_counter()
    spark.read.parquet(str(config.RATINGS_PARQUET)).count()
    parquet_read_seconds = time.perf_counter() - t0

    return {
        "dataset": config.DATASET,
        "n_ratings": n_ratings,
        "csv_bytes": csv_bytes,
        "parquet_bytes": _dir_bytes(config.RATINGS_PARQUET),
        "infer_schema_seconds": round(infer_schema_seconds, 3),
        "csv_read_seconds": round(csv_read_seconds, 3),
        "parquet_read_seconds": round(parquet_read_seconds, 3),
        "n_output_files": n_files,
    }


def main() -> None:
    spark = get_spark("ingest")
    stats = ingest(spark)
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.RESULTS_DIR / "ingest_stats.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=list(stats))
        writer.writeheader()
        writer.writerow(stats)
    print(f"Ghi thống kê ingest: {out}")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_ingest.py`
Expected: 3 PASS

- [ ] **Step 6: Chạy job thật trên bộ nhỏ**

```bash
docker compose -f docker/docker-compose.yml exec -T spark-master \
  env DATASET=ml-latest-small SPARK_MASTER_URL=spark://spark-master:7077 \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/src/jobs/ingest.py
```

Expected: in ra bảng thống kê, `parquet_bytes` nhỏ hơn `csv_bytes` rõ rệt, file `report/results/ingest_stats.csv` được tạo.

- [ ] **Step 7: Commit**

```bash
git add src/common/schema.py src/jobs/ingest.py tests/test_ingest.py
git commit -m "feat: job ingest CSV sang Parquet kèm đo thống kê"
```

---

## Task 3: Chia tập theo thời gian

**Files:**
- Create: `src/common/split.py`
- Test: `tests/test_split.py`

**Interfaces:**
- Consumes: `src.config`
- Produces: `src.common.split.add_split_column(df: DataFrame, train_frac: float = config.SPLIT_TRAIN, val_frac: float = config.SPLIT_VAL, min_ratings: int = config.MIN_RATINGS_PER_USER) -> DataFrame` — thêm cột `split` nhận một trong `"train"`, `"val"`, `"test"`; giữ nguyên mọi cột đầu vào.
- Produces: `src.common.split.count_excluded_users(df: DataFrame, min_ratings: int) -> int`

- [ ] **Step 1: Viết test thất bại `tests/test_split.py`**

```python
from src.common.split import add_split_column, count_excluded_users

SCHEMA = "userId int, movieId int, rating double, timestamp long"


def test_newest_ratings_go_to_test(spark):
    # user 1 có 10 rating, timestamp tăng dần theo movieId
    rows = [(1, i, 4.0, 1000 + i) for i in range(1, 11)]
    df = spark.createDataFrame(rows, SCHEMA)

    by_movie = {r["movieId"]: r["split"] for r in add_split_column(df).collect()}

    # percent_rank của 10 dòng là 0, 1/9, 2/9, ..., 9/9
    assert by_movie[1] == "train"    # 0.000
    assert by_movie[7] == "train"    # 6/9 = 0.667 < 0.70
    assert by_movie[8] == "val"      # 7/9 = 0.778, trong [0.70, 0.85)
    assert by_movie[9] == "test"     # 8/9 = 0.889 >= 0.85
    assert by_movie[10] == "test"    # 1.000


def test_users_below_min_ratings_all_go_to_train(spark):
    rows = [(2, i, 5.0, 2000 + i) for i in range(1, 4)]  # chỉ 3 rating
    df = spark.createDataFrame(rows, SCHEMA)

    splits = {r["split"] for r in add_split_column(df).collect()}

    assert splits == {"train"}


def test_split_is_per_user_not_global(spark):
    # user 3 rating rất sớm, user 4 rating rất muộn. Nếu chia theo thời gian
    # toàn cục thì toàn bộ user 3 vào train và toàn bộ user 4 vào test.
    rows = [(3, i, 4.0, 100 + i) for i in range(1, 11)]
    rows += [(4, i, 4.0, 900_000 + i) for i in range(1, 11)]
    df = spark.createDataFrame(rows, SCHEMA)

    result = add_split_column(df)

    for user in (3, 4):
        splits = {r["split"] for r in result.filter(f"userId = {user}").collect()}
        assert splits == {"train", "val", "test"}


def test_input_columns_are_preserved(spark):
    df = spark.createDataFrame([(1, i, 4.0, 1000 + i) for i in range(1, 11)], SCHEMA)
    result = add_split_column(df)
    assert set(df.columns).issubset(set(result.columns))
    assert "split" in result.columns


def test_count_excluded_users(spark):
    rows = [(5, i, 4.0, 1000 + i) for i in range(1, 11)]   # đủ điều kiện
    rows += [(6, i, 4.0, 2000 + i) for i in range(1, 3)]   # chỉ 2 rating
    df = spark.createDataFrame(rows, SCHEMA)

    assert count_excluded_users(df, min_ratings=5) == 1


def test_split_is_deterministic_when_timestamps_tie(spark):
    """Tie-break theo movieId phải cho kết quả giống hệt nhau qua nhiều lần chạy.

    MovieLens có rất nhiều user chấm hàng loạt phim trong cùng một phiên, nên
    trùng timestamp là chuyện thường. Thiếu tie-break, Spark tự do sắp xếp các
    dòng trùng khác nhau ở mỗi lần chạy, và cùng một rating có thể rơi vào
    train lần này, test lần sau — kết quả tuning ở Task 6 mất tính tái lập mà
    không có gì báo lỗi.
    """
    # 10 rating, TẤT CẢ cùng timestamp
    rows = [(7, movie_id, 4.0, 5000) for movie_id in range(1, 11)]
    df = spark.createDataFrame(rows, SCHEMA)

    first = {r["movieId"]: r["split"] for r in add_split_column(df).collect()}
    second = {r["movieId"]: r["split"] for r in add_split_column(df).collect()}

    assert first == second
    # Thứ tự do movieId quyết định, nên phân bố phải giống hệt trường hợp
    # timestamp tăng dần: movieId nhỏ nhất vào train, lớn nhất vào test.
    assert first[1] == "train"
    assert first[10] == "test"
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_split.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.split'`

- [ ] **Step 3: Viết `src/common/split.py`**

```python
"""Chia tập train/validation/test theo thời gian trong từng user.

KHÔNG dùng randomSplit: nó cho mô hình nhìn thấy đánh giá tương lai của một
user rồi bắt dự đoán đánh giá quá khứ của chính user đó — rò rỉ dữ liệu, và
làm điểm số đẹp một cách giả tạo.

Phải có tập validation tách khỏi test: nếu chọn siêu tham số trên chính tập
test rồi báo cáo điểm trên tập đó, con số sẽ lạc quan hơn thực tế.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src import config


def add_split_column(
    df: DataFrame,
    train_frac: float = config.SPLIT_TRAIN,
    val_frac: float = config.SPLIT_VAL,
    min_ratings: int = config.MIN_RATINGS_PER_USER,
) -> DataFrame:
    """Thêm cột `split` với giá trị "train" | "val" | "test".

    User có dưới `min_ratings` rating thì không chia được thành ba phần có
    nghĩa — toàn bộ rating của họ vào train và họ bị loại khỏi phần đánh giá.
    """
    # Sắp xếp phụ theo movieId để kết quả tất định khi trùng timestamp
    order = Window.partitionBy("userId").orderBy("timestamp", "movieId")
    per_user = Window.partitionBy("userId")

    ranked = (
        df.withColumn("_pct", F.percent_rank().over(order))
          .withColumn("_n", F.count(F.lit(1)).over(per_user))
    )

    return (
        ranked.withColumn(
            "split",
            F.when(F.col("_n") < min_ratings, F.lit("train"))
             .when(F.col("_pct") < train_frac, F.lit("train"))
             .when(F.col("_pct") < val_frac, F.lit("val"))
             .otherwise(F.lit("test")),
        )
        .drop("_pct", "_n")
    )


def count_excluded_users(df: DataFrame, min_ratings: int = config.MIN_RATINGS_PER_USER) -> int:
    """Số user bị loại khỏi đánh giá vì có quá ít rating.

    Con số này phải nêu trong báo cáo: nó chính là biểu hiện định lượng của
    bài toán cold-start trên bộ dữ liệu.
    """
    return (
        df.groupBy("userId")
          .count()
          .filter(F.col("count") < min_ratings)
          .count()
    )
```

- [ ] **Step 4: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_split.py`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add src/common/split.py tests/test_split.py
git commit -m "feat: chia tập theo thời gian trong từng user, 70/15/15"
```

---

## Task 4: Module metrics xếp hạng

**Files:**
- Create: `src/common/metrics.py`
- Test: `tests/test_metrics.py`

**Interfaces:**
- Consumes: không có (Python thuần, **không import pyspark**)
- Produces:
  - `precision_at_k(recommended: Sequence, relevant: Set, k: int) -> float`
  - `recall_at_k(recommended: Sequence, relevant: Set, k: int) -> float`
  - `dcg_at_k(recommended: Sequence, relevant: Set, k: int) -> float`
  - `ndcg_at_k(recommended: Sequence, relevant: Set, k: int) -> float`
  - `coverage(recommended_items: Iterable, catalog_size: int) -> float`

- [ ] **Step 1: Viết test thất bại `tests/test_metrics.py`**

```python
import math

import pytest

from src.common.metrics import (
    coverage,
    dcg_at_k,
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
)


def test_precision_counts_hits_over_k():
    # gợi ý [1,2,3,4], liên quan {1,3} -> 2 trúng / 4 = 0.5
    assert precision_at_k([1, 2, 3, 4], {1, 3}, k=4) == 0.5


def test_precision_divides_by_k_not_list_length():
    # Chỉ gợi ý được 1 phim nhưng k=10: precision là 1/10, không phải 1/1.
    # Chia cho len(list) sẽ thưởng cho mô hình gợi ý ít — đó là cài sai.
    assert precision_at_k([1], {1}, k=10) == pytest.approx(0.1)


def test_recall_divides_by_number_of_relevant():
    # liên quan {1,3,5}, gợi ý bắt được 1 và 3 -> 2/3
    assert recall_at_k([1, 2, 3, 4], {1, 3, 5}, k=4) == pytest.approx(2 / 3)


def test_dcg_discounts_by_position():
    # vị trí 0 -> 1/log2(2) = 1.0 ; vị trí 2 -> 1/log2(4) = 0.5
    assert dcg_at_k([1, 2, 3], {1, 3}, k=3) == pytest.approx(1.5)


def test_ndcg_hand_computed():
    # gợi ý [1,2,3], liên quan {1,3}
    # DCG  = 1/log2(2) + 0 + 1/log2(4) = 1.0 + 0.5      = 1.5
    # IDCG = 1/log2(2) + 1/log2(3)     = 1.0 + 0.6309298 = 1.6309298
    # NDCG = 1.5 / 1.6309298 = 0.9197208
    assert ndcg_at_k([1, 2, 3], {1, 3}, k=3) == pytest.approx(0.9197208, abs=1e-6)


def test_ndcg_is_one_when_relevant_items_are_ranked_first():
    assert ndcg_at_k([1, 3, 2], {1, 3}, k=3) == pytest.approx(1.0)


def test_ndcg_idcg_is_capped_at_k():
    # 5 phim liên quan nhưng k=2: IDCG chỉ tính 2 vị trí, nên gợi ý 2 phim
    # liên quan đầu bảng vẫn phải cho NDCG = 1.0
    assert ndcg_at_k([1, 2], {1, 2, 3, 4, 5}, k=2) == pytest.approx(1.0)


def test_metrics_are_zero_when_nothing_is_relevant():
    assert precision_at_k([1, 2], set(), k=2) == 0.0
    assert recall_at_k([1, 2], set(), k=2) == 0.0
    assert ndcg_at_k([1, 2], set(), k=2) == 0.0


def test_metrics_handle_empty_recommendations():
    assert precision_at_k([], {1}, k=10) == 0.0
    assert recall_at_k([], {1}, k=10) == 0.0
    assert ndcg_at_k([], {1}, k=10) == 0.0


def test_coverage_counts_distinct_items_over_catalog():
    assert coverage([1, 1, 2, 3], catalog_size=10) == pytest.approx(0.3)


def test_coverage_of_empty_catalog_is_zero():
    assert coverage([1, 2], catalog_size=0) == 0.0
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_metrics.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.metrics'`

- [ ] **Step 3: Viết `src/common/metrics.py`**

```python
"""Metrics xếp hạng cho hệ gợi ý.

Python thuần, cố ý KHÔNG phụ thuộc pyspark: đây là phần logic dễ cài sai nhất
mà vẫn cho ra con số trông hợp lý, nên nó phải test được trong một giây.

Quy ước độ liên quan là nhị phân: một phim là "liên quan" nếu user chấm
>= config.RELEVANCE_THRESHOLD (4.0 sao). Việc chọn ngưỡng nằm ở phía gọi hàm.
"""
import math
from typing import Iterable, Sequence, Set


def _hits(recommended: Sequence, relevant: Set, k: int) -> int:
    """Đếm số phim liên quan PHÂN BIỆT nằm trong top-k.

    Đếm phân biệt chứ không đếm số lần xuất hiện: nếu danh sách gợi ý lỡ chứa
    một phim hai lần, cách đếm theo lần xuất hiện sẽ cho recall_at_k([1,1],{1},k=2)
    = 2/1 = 2.0, tức vượt khoảng [0,1] hợp lệ mà không có gì báo lỗi. Danh sách
    top-k của ALS không trùng lặp, nhưng metrics này sinh số cho báo cáo nên
    không được phép trả về giá trị vô nghĩa dù đầu vào có sai.
    """
    return len({item for item in list(recommended)[:k] if item in relevant})


def precision_at_k(recommended: Sequence, relevant: Set, k: int) -> float:
    """Tỷ lệ phim liên quan trong top-k.

    Mẫu số là k, KHÔNG phải len(recommended): chia cho độ dài danh sách sẽ
    thưởng cho mô hình gợi ý ít phim.
    """
    if k <= 0 or not relevant:
        return 0.0
    return _hits(recommended, relevant, k) / k


def recall_at_k(recommended: Sequence, relevant: Set, k: int) -> float:
    """Tỷ lệ phim liên quan của user được bắt trúng trong top-k."""
    if k <= 0 or not relevant:
        return 0.0
    return _hits(recommended, relevant, k) / len(relevant)


def dcg_at_k(recommended: Sequence, relevant: Set, k: int) -> float:
    """Discounted Cumulative Gain với độ liên quan nhị phân."""
    if k <= 0:
        return 0.0
    return sum(
        1.0 / math.log2(position + 2)
        for position, item in enumerate(list(recommended)[:k])
        if item in relevant
    )


def ndcg_at_k(recommended: Sequence, relevant: Set, k: int) -> float:
    """DCG chuẩn hoá theo thứ tự lý tưởng.

    IDCG bị chặn ở min(k, số phim liên quan): nếu user có 50 phim liên quan
    mà k=10, danh sách hoàn hảo chỉ có thể chứa 10 phim, nên IDCG phải tính
    trên 10 vị trí. Không chặn ở k là lỗi phổ biến làm NDCG luôn nhỏ hơn 1.
    """
    if k <= 0 or not relevant:
        return 0.0
    ideal_positions = min(k, len(relevant))
    idcg = sum(1.0 / math.log2(position + 2) for position in range(ideal_positions))
    if idcg == 0.0:
        return 0.0
    return dcg_at_k(recommended, relevant, k) / idcg


def coverage(recommended_items: Iterable, catalog_size: int) -> float:
    """Tỷ lệ catalog mà hệ thống thực sự gợi ý tới.

    Phát hiện mô hình chỉ quanh quẩn vài phim nổi tiếng: RMSE có thể rất đẹp
    trong khi coverage chỉ 2%, nghĩa là mọi user đều nhận cùng một danh sách.
    """
    if catalog_size <= 0:
        return 0.0
    return len(set(recommended_items)) / catalog_size
```

- [ ] **Step 4: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_metrics.py`
Expected: 11 PASS

- [ ] **Step 5: Commit**

```bash
git add src/common/metrics.py tests/test_metrics.py
git commit -m "feat: metrics Precision/Recall/NDCG/Coverage kèm test tính tay"
```

---

## Task 5: Ba baseline so sánh

**Files:**
- Create: `src/common/baselines.py`
- Test: `tests/test_baselines.py`

**Interfaces:**
- Consumes: `src.config`
- Produces:
  - `global_mean_predictions(train: DataFrame, target: DataFrame) -> DataFrame` — trả về `target` kèm cột `prediction`
  - `item_mean_predictions(train: DataFrame, target: DataFrame) -> DataFrame` — cùng dạng; phim chưa thấy trong train lấy điểm trung bình toàn cục
  - `popularity_top_n(train: DataFrame, n: int) -> list[int]` — danh sách `movieId` xếp theo số lượt rating giảm dần

- [ ] **Step 1: Viết test thất bại `tests/test_baselines.py`**

```python
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
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_baselines.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.baselines'`

- [ ] **Step 3: Viết `src/common/baselines.py`**

```python
"""Ba baseline để đối chiếu với ALS.

Baseline popularity là cái quan trọng nhất: trong recsys nó mạnh một cách
đáng ngạc nhiên, và một mô hình thua nó là mô hình vô dụng. Nhiều đồ án
hoàn thành rồi mới phát hiện điều này.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def global_mean_predictions(train: DataFrame, target: DataFrame) -> DataFrame:
    """Luôn dự đoán điểm trung bình toàn cục của tập train."""
    mean = train.agg(F.avg("rating")).first()[0]
    return target.withColumn("prediction", F.lit(float(mean)))


def item_mean_predictions(train: DataFrame, target: DataFrame) -> DataFrame:
    """Dự đoán bằng điểm trung bình của chính bộ phim đó.

    Phim chưa từng xuất hiện trong train lấy trung bình toàn cục làm dự phòng
    — nếu để null thì RMSE sẽ ra NaN, đúng cái bẫy mà coldStartStrategy="drop"
    xử lý cho phía ALS.
    """
    global_mean = float(train.agg(F.avg("rating")).first()[0])
    item_means = train.groupBy("movieId").agg(F.avg("rating").alias("_item_mean"))
    return (
        target.join(item_means, on="movieId", how="left")
        .withColumn("prediction", F.coalesce(F.col("_item_mean"), F.lit(global_mean)))
        .drop("_item_mean")
    )


def popularity_top_n(train: DataFrame, n: int) -> list:
    """Top-n phim theo SỐ LƯỢT rating, không theo điểm trung bình.

    Xếp theo điểm trung bình sẽ đẩy lên đầu những phim chỉ có 1-2 lượt đánh
    giá 5 sao — đó không phải là "phổ biến".
    """
    rows = (
        train.groupBy("movieId")
        .agg(F.count(F.lit(1)).alias("_n"))
        .orderBy(F.col("_n").desc(), F.col("movieId").asc())
        .limit(n)
        .collect()
    )
    return [row["movieId"] for row in rows]
```

- [ ] **Step 4: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_baselines.py`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add src/common/baselines.py tests/test_baselines.py
git commit -m "feat: ba baseline global-mean, item-mean, popularity"
```

---

## Task 6: Huấn luyện ALS và grid search

**Files:**
- Create: `src/jobs/train_als.py`
- Test: `tests/test_train_als.py`

**Interfaces:**
- Consumes: `src.config`, `src.session.get_spark`, `src.common.split.add_split_column`
- Produces:
  - `build_als(rank: int, reg_param: float, max_iter: int = config.ALS_MAX_ITER) -> ALS`
  - `evaluate_rmse(model, df: DataFrame) -> float`
  - `grid_search(train: DataFrame, validation: DataFrame, ranks: list[int], reg_params: list[float]) -> list[dict]` — mỗi dict có khoá `rank`, `regParam`, `maxIter`, `rmse`, `fit_seconds`
  - `main()` — ghi `report/results/tuning.csv` và lưu mô hình tốt nhất (đã huấn luyện lại trên train+validation) vào `config.MODEL_DIR`

- [ ] **Step 1: Viết test thất bại `tests/test_train_als.py`**

```python
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
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_train_als.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.jobs.train_als'`

- [ ] **Step 3: Viết `src/jobs/train_als.py`**

```python
"""Job 2: huấn luyện ALS và chọn siêu tham số trên tập validation.

Siêu tham số được chọn trên tập VALIDATION, không phải tập test. Sau khi chốt,
mô hình được huấn luyện lại trên train + validation rồi mới giao cho Job 3
chấm trên test đúng một lần.
"""
import csv as csv_module
import time

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame

from src import config
from src.common.split import add_split_column, count_excluded_users
from src.session import get_spark


def build_als(rank: int, reg_param: float, max_iter: int = config.ALS_MAX_ITER) -> ALS:
    """Tạo ALS với hai tham số bắt buộc mà thiếu là hỏng.

    coldStartStrategy="drop": không có nó, user hoặc phim chưa xuất hiện trong
    train cho dự đoán NaN, và RMSE của toàn bộ tập test trở thành NaN.

    checkpointInterval: ALS lặp nhiều vòng sinh lineage RDD rất dài, dẫn tới
    StackOverflowError. Checkpoint định kỳ cắt lineage.
    """
    return ALS(
        rank=rank,
        regParam=reg_param,
        maxIter=max_iter,
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        coldStartStrategy="drop",
        checkpointInterval=config.ALS_CHECKPOINT_INTERVAL,
        nonnegative=False,
        seed=42,
    )


def evaluate_rmse(model: ALSModel, df: DataFrame) -> float:
    predictions = model.transform(df)
    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    )
    return float(evaluator.evaluate(predictions))


def grid_search(
    train: DataFrame,
    validation: DataFrame,
    ranks: list = None,
    reg_params: list = None,
) -> list:
    """Thử mọi tổ hợp, chấm trên validation, trả về kết quả đã sắp theo RMSE."""
    ranks = ranks if ranks is not None else config.ALS_RANKS
    reg_params = reg_params if reg_params is not None else config.ALS_REG_PARAMS

    train.cache()
    validation.cache()

    results = []
    for rank in ranks:
        for reg_param in reg_params:
            started = time.perf_counter()
            model = build_als(rank, reg_param).fit(train)
            fit_seconds = time.perf_counter() - started
            rmse = evaluate_rmse(model, validation)
            print(f"rank={rank} regParam={reg_param} -> RMSE={rmse:.4f} ({fit_seconds:.1f}s)")
            results.append({
                "rank": rank,
                "regParam": reg_param,
                "maxIter": config.ALS_MAX_ITER,
                "rmse": round(rmse, 5),
                "fit_seconds": round(fit_seconds, 1),
            })

    return sorted(results, key=lambda row: row["rmse"])


def main() -> None:
    spark = get_spark("train_als")

    ratings = spark.read.parquet(str(config.RATINGS_PARQUET))
    excluded = count_excluded_users(ratings)
    print(f"User bị loại khỏi đánh giá (dưới {config.MIN_RATINGS_PER_USER} rating): {excluded}")

    split = add_split_column(ratings)
    train = split.filter("split = 'train'").select("userId", "movieId", "rating")
    validation = split.filter("split = 'val'").select("userId", "movieId", "rating")

    results = grid_search(train, validation)

    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    tuning_csv = config.RESULTS_DIR / "tuning.csv"
    with open(tuning_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    print(f"Ghi kết quả tuning: {tuning_csv}")

    best = results[0]
    print(f"Tổ hợp tốt nhất: rank={best['rank']} regParam={best['regParam']} RMSE={best['rmse']}")

    # Huấn luyện lại trên train + validation trước khi giao cho Job 3
    refit_data = train.union(validation)
    final_model = build_als(best["rank"], best["regParam"]).fit(refit_data)
    final_model.write().overwrite().save(str(config.MODEL_DIR))
    print(f"Lưu mô hình: {config.MODEL_DIR}")

    with open(config.RESULTS_DIR / "best_params.csv", "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=list(best) + ["excluded_users"])
        writer.writeheader()
        writer.writerow({**best, "excluded_users": excluded})

    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_train_als.py`
Expected: 6 PASS

- [ ] **Step 5: Chạy job trên bộ nhỏ**

```bash
docker compose -f docker/docker-compose.yml exec -T spark-master \
  env DATASET=ml-latest-small SPARK_MASTER_URL=spark://spark-master:7077 \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/src/jobs/train_als.py
```

Expected: in ra 9 dòng kết quả, tạo `report/results/tuning.csv` và `data/output/model/`. Kiểm tra không có RMSE nào là `nan`.

- [ ] **Step 6: Commit**

```bash
git add src/jobs/train_als.py tests/test_train_als.py
git commit -m "feat: huấn luyện ALS với grid search trên tập validation"
```

---

## Task 7: Đánh giá trên tập test kèm so sánh baseline

**Files:**
- Create: `src/jobs/evaluate.py`
- Test: `tests/test_evaluate.py`

**Interfaces:**
- Consumes: `src.common.metrics`, `src.common.baselines`, `src.common.split`, `src.jobs.train_als.build_als`
- Produces:
  - `relevant_items(test: DataFrame, threshold: float = config.RELEVANCE_THRESHOLD) -> DataFrame` — cột `userId`, `relevant` (array)
  - `ranking_metrics(recs: DataFrame, actual: DataFrame, k: int) -> dict` — khoá `precision_at_k`, `recall_at_k`, `ndcg_at_k`
  - `rating_metrics(predictions: DataFrame) -> dict` — khoá `rmse`, `mae`
  - `main()` — ghi `report/results/metrics.csv` với một dòng cho mỗi mô hình (`als`, `global_mean`, `item_mean`, `popularity`)

- [ ] **Step 1: Viết test thất bại `tests/test_evaluate.py`**

```python
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
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_evaluate.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.jobs.evaluate'`

- [ ] **Step 3: Viết `src/jobs/evaluate.py`**

```python
"""Job 3: chấm mô hình tốt nhất trên tập TEST, một lần duy nhất.

RMSE đo sai lệch của điểm dự đoán, không đo chất lượng của danh sách gợi ý —
một mô hình RMSE đẹp vẫn có thể gợi ý danh sách vô dụng. Vì vậy báo cáo cần
cả metrics xếp hạng lẫn đối chiếu với baseline.
"""
import csv as csv_module

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src import config
from src.common import metrics as M
from src.common.baselines import (
    global_mean_predictions,
    item_mean_predictions,
    popularity_top_n,
)
from src.common.split import add_split_column
from src.session import get_spark


def relevant_items(test: DataFrame, threshold: float = config.RELEVANCE_THRESHOLD) -> DataFrame:
    """Ground truth: các phim user chấm >= ngưỡng.

    User không có phim nào đạt ngưỡng sẽ biến mất khỏi kết quả — đúng như
    mong muốn, vì không có gì để so sánh thì không chấm được.
    """
    return (
        test.filter(F.col("rating") >= threshold)
        .groupBy("userId")
        .agg(F.collect_set("movieId").alias("relevant"))
    )


def rating_metrics(predictions: DataFrame) -> dict:
    result = {}
    for name in ("rmse", "mae"):
        evaluator = RegressionEvaluator(
            metricName=name, labelCol="rating", predictionCol="prediction"
        )
        result[name] = round(float(evaluator.evaluate(predictions)), 5)
    return result


def ranking_metrics(recs: DataFrame, actual: DataFrame, k: int = config.TOP_K) -> dict:
    """Trung bình Precision/Recall/NDCG@k trên các user có ground truth."""
    schema = T.StructType([
        T.StructField("precision", T.DoubleType()),
        T.StructField("recall", T.DoubleType()),
        T.StructField("ndcg", T.DoubleType()),
    ])

    @F.udf(returnType=schema)
    def _row_metrics(items, relevant):
        recommended = list(items or [])
        truth = set(relevant or [])
        return (
            M.precision_at_k(recommended, truth, k),
            M.recall_at_k(recommended, truth, k),
            M.ndcg_at_k(recommended, truth, k),
        )

    scored = recs.join(actual, on="userId", how="inner").withColumn(
        "m", _row_metrics(F.col("items"), F.col("relevant"))
    )
    row = scored.agg(
        F.avg("m.precision").alias("precision_at_k"),
        F.avg("m.recall").alias("recall_at_k"),
        F.avg("m.ndcg").alias("ndcg_at_k"),
    ).first()

    return {
        "precision_at_k": round(float(row["precision_at_k"] or 0.0), 5),
        "recall_at_k": round(float(row["recall_at_k"] or 0.0), 5),
        "ndcg_at_k": round(float(row["ndcg_at_k"] or 0.0), 5),
    }


def _coverage_of(recs: DataFrame, catalog_size: int) -> float:
    distinct_items = (
        recs.select(F.explode("items").alias("movieId")).select("movieId").distinct().count()
    )
    return round(distinct_items / catalog_size, 5) if catalog_size else 0.0


def main() -> None:
    spark = get_spark("evaluate")

    ratings = spark.read.parquet(str(config.RATINGS_PARQUET))
    split = add_split_column(ratings)
    train_val = split.filter("split IN ('train', 'val')").select("userId", "movieId", "rating")
    test = split.filter("split = 'test'").select("userId", "movieId", "rating")

    catalog_size = ratings.select("movieId").distinct().count()
    actual = relevant_items(test).cache()
    test_users = actual.select("userId")
    k = config.TOP_K

    model = ALSModel.load(str(config.MODEL_DIR))
    rows = []

    # --- ALS ---
    als_preds = model.transform(test)
    als_recs = (
        model.recommendForUserSubset(test_users, k)
        .select("userId", F.col("recommendations.movieId").alias("items"))
    )
    rows.append({
        "model": "als",
        **rating_metrics(als_preds),
        **ranking_metrics(als_recs, actual, k),
        "coverage": _coverage_of(als_recs, catalog_size),
    })

    # --- Baseline dự đoán điểm ---
    rows.append({
        "model": "global_mean",
        **rating_metrics(global_mean_predictions(train_val, test)),
        "precision_at_k": None, "recall_at_k": None, "ndcg_at_k": None, "coverage": None,
    })
    rows.append({
        "model": "item_mean",
        **rating_metrics(item_mean_predictions(train_val, test)),
        "precision_at_k": None, "recall_at_k": None, "ndcg_at_k": None, "coverage": None,
    })

    # --- Baseline popularity: cùng một danh sách cho mọi user ---
    top_movies = popularity_top_n(train_val, k)
    popularity_recs = test_users.withColumn(
        "items", F.array(*[F.lit(movie_id) for movie_id in top_movies])
    )
    rows.append({
        "model": "popularity",
        "rmse": None, "mae": None,
        **ranking_metrics(popularity_recs, actual, k),
        "coverage": _coverage_of(popularity_recs, catalog_size),
    })

    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.RESULTS_DIR / "metrics.csv"
    fieldnames = ["model", "rmse", "mae", "precision_at_k", "recall_at_k", "ndcg_at_k", "coverage"]
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})

    print(f"Ghi metrics: {out}  (K={k}, ngưỡng liên quan={config.RELEVANCE_THRESHOLD})")
    for row in rows:
        print(f"  {row['model']}: {row}")

    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_evaluate.py`
Expected: 5 PASS

- [ ] **Step 5: Chạy job trên bộ nhỏ và đọc kỹ kết quả**

```bash
docker compose -f docker/docker-compose.yml exec -T spark-master \
  env DATASET=ml-latest-small SPARK_MASTER_URL=spark://spark-master:7077 \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/src/jobs/evaluate.py
```

Expected: `report/results/metrics.csv` có 4 dòng. **So sánh dòng `als` với dòng `popularity` trên `ndcg_at_k`.** Nếu ALS thua, đó là một kết quả hợp lệ cần phân tích trong báo cáo, không phải lỗi cần giấu.

- [ ] **Step 6: Commit**

```bash
git add src/jobs/evaluate.py tests/test_evaluate.py
git commit -m "feat: đánh giá trên tập test kèm so sánh ba baseline"
```

---

## Task 8: Xuất gợi ý sang SQLite và item factors

**Files:**
- Create: `src/jobs/export_recs.py`
- Test: `tests/test_export_recs.py`

**Interfaces:**
- Consumes: `src.config`
- Produces:
  - `write_sqlite(recs_pdf: pandas.DataFrame, movies_pdf: pandas.DataFrame, ratings_sample_pdf: pandas.DataFrame, db_path: Path) -> None` — tạo ba bảng: `recommendations(userId, movieId, rank, score)` và `ratings_sample(userId, movieId, rating)` đều có index trên `userId`, cùng `movies(movieId, title, genres)`
  - `main()` — ghi `config.RECS_PARQUET`, `config.RECS_SQLITE`, `config.ITEM_FACTORS_NPY`, `config.ITEM_INDEX_PARQUET`

Bảng `ratings_sample` phải có ngay từ task này vì `Store.history()` ở Task 9 đọc nó, và giao diện ở Task 10 hiển thị nó.

- [ ] **Step 1: Viết test thất bại `tests/test_export_recs.py`**

```python
import sqlite3

import pandas as pd

from src.jobs.export_recs import write_sqlite


def _recs_frame():
    return pd.DataFrame({
        "userId": [1, 1, 2],
        "movieId": [10, 11, 10],
        "rank": [1, 2, 1],
        "score": [4.9, 4.2, 3.8],
    })


def _movies_frame():
    return pd.DataFrame({
        "movieId": [10, 11],
        "title": ["Phim A (1999)", "Phim B (2005)"],
        "genres": ["Action", "Comedy"],
    })


def _ratings_frame():
    return pd.DataFrame({
        "userId": [1, 1, 2],
        "movieId": [10, 11, 10],
        "rating": [5.0, 4.0, 3.0],
    })


def test_write_sqlite_creates_all_three_tables(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"recommendations", "movies", "ratings_sample"} <= tables


def test_write_sqlite_indexes_both_user_id_columns(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_recommendations_userId" in indexes
    assert "idx_ratings_sample_userId" in indexes


def test_recommendations_are_queryable_in_rank_order(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT movieId FROM recommendations WHERE userId = 1 ORDER BY rank"
        ).fetchall()
    assert [r[0] for r in rows] == [10, 11]


def test_ratings_sample_is_queryable_by_user(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT movieId FROM ratings_sample WHERE userId = 1 ORDER BY rating DESC"
        ).fetchall()
    assert [r[0] for r in rows] == [10, 11]


def test_write_sqlite_is_idempotent(tmp_path):
    db = tmp_path / "recs.sqlite"

    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)
    write_sqlite(_recs_frame(), _movies_frame(), _ratings_frame(), db)

    with sqlite3.connect(db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
    assert count == 3
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_export_recs.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.jobs.export_recs'`

- [ ] **Step 3: Viết `src/jobs/export_recs.py`**

```python
"""Job 4: xuất kết quả batch sang dạng tầng serving đọc được.

Đây là ranh giới bàn giao giữa batch và serving. Sau job này, tầng serving
chỉ cần SQLite và một mảng numpy — không cần Spark, không cần Java.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import Window
from pyspark.sql import functions as F

from src import config
from src.session import get_spark

HISTORY_PER_USER = 20


def write_sqlite(
    recs_pdf: pd.DataFrame,
    movies_pdf: pd.DataFrame,
    ratings_sample_pdf: pd.DataFrame,
    db_path: Path,
) -> None:
    """Ghi gợi ý, mẫu rating và metadata phim vào SQLite, có index trên userId.

    if_exists="replace" để chạy lại job không nhân đôi dữ liệu.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        recs_pdf.to_sql("recommendations", conn, if_exists="replace", index=False)
        movies_pdf.to_sql("movies", conn, if_exists="replace", index=False)
        ratings_sample_pdf.to_sql("ratings_sample", conn, if_exists="replace", index=False)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_recommendations_userId "
            "ON recommendations(userId)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ratings_sample_userId "
            "ON ratings_sample(userId)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_movies_movieId ON movies(movieId)")
        conn.commit()


def top_rated_per_user(ratings, n: int = HISTORY_PER_USER):
    """Giữ n phim user chấm cao nhất, để giao diện đối chiếu với gợi ý.

    Không xuất toàn bộ 25 triệu rating sang SQLite: giao diện chỉ cần đủ để
    người xem thấy sở thích của user, và file SQLite phải đủ nhỏ để nạp nhanh.
    """
    order = Window.partitionBy("userId").orderBy(F.col("rating").desc(), F.col("movieId"))
    return (
        ratings.withColumn("_r", F.row_number().over(order))
        .filter(F.col("_r") <= n)
        .select("userId", "movieId", "rating")
    )


def main() -> None:
    spark = get_spark("export_recs")

    model = ALSModel.load(str(config.MODEL_DIR))
    movies = spark.read.parquet(str(config.MOVIES_PARQUET))
    ratings = spark.read.parquet(str(config.RATINGS_PARQUET))

    recs = (
        model.recommendForAllUsers(config.N_RECOMMENDATIONS)
        .select("userId", F.posexplode("recommendations").alias("pos", "rec"))
        .select(
            "userId",
            F.col("rec.movieId").alias("movieId"),
            (F.col("pos") + 1).alias("rank"),
            F.col("rec.rating").cast("double").alias("score"),
        )
    )
    recs.write.mode("overwrite").parquet(str(config.RECS_PARQUET))
    print(f"Ghi Parquet gợi ý: {config.RECS_PARQUET}")

    recs_pdf = spark.read.parquet(str(config.RECS_PARQUET)).toPandas()
    movies_pdf = movies.toPandas()
    ratings_sample_pdf = top_rated_per_user(ratings).toPandas()
    write_sqlite(recs_pdf, movies_pdf, ratings_sample_pdf, config.RECS_SQLITE)
    print(
        f"Ghi SQLite: {config.RECS_SQLITE} "
        f"({len(recs_pdf):,} gợi ý, {len(ratings_sample_pdf):,} rating mẫu)"
    )

    # Item factors cho chức năng "phim tương tự"
    factors_pdf = model.itemFactors.toPandas().sort_values("id").reset_index(drop=True)
    matrix = np.array(factors_pdf["features"].tolist(), dtype=np.float32)
    np.save(config.ITEM_FACTORS_NPY, matrix)
    factors_pdf[["id"]].rename(columns={"id": "movieId"}).to_parquet(
        config.ITEM_INDEX_PARQUET, index=False
    )
    print(f"Ghi item factors: {config.ITEM_FACTORS_NPY} shape={matrix.shape}")

    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_export_recs.py`
Expected: 5 PASS

- [ ] **Step 5: Chạy job trên bộ nhỏ**

```bash
docker compose -f docker/docker-compose.yml exec -T spark-master \
  env DATASET=ml-latest-small SPARK_MASTER_URL=spark://spark-master:7077 \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/src/jobs/export_recs.py
```

Expected: tạo `data/output/recs.sqlite`, `item_factors.npy`, `item_index.parquet`.

- [ ] **Step 6: Commit**

```bash
git add src/jobs/export_recs.py tests/test_export_recs.py
git commit -m "feat: xuất gợi ý sang SQLite và item factors sang npy"
```

---

## Task 9: API phục vụ gợi ý

**Files:**
- Create: `serving/__init__.py`, `serving/store.py`, `serving/api.py`
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: các file do Task 8 sinh ra (`recs.sqlite`, `item_factors.npy`, `item_index.parquet`)
- Produces:
  - `serving.store.Store(db_path, factors_path, index_path)` với các phương thức `recommendations(user_id, k) -> list[dict]`, `history(user_id, k) -> list[dict]`, `similar(movie_id, k) -> list[dict]`, `search(query, limit) -> list[dict]`, `stats() -> dict`
  - `serving.api.app` — ứng dụng FastAPI

**Ràng buộc:** file trong `serving/` **không được import pyspark**. Image `Dockerfile.app` không có Java nên vi phạm sẽ làm container chết ngay.

- [ ] **Step 1: Viết test thất bại `tests/test_api.py`**

```python
import sqlite3

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def store_paths(tmp_path):
    db = tmp_path / "recs.sqlite"
    recs = pd.DataFrame({
        "userId": [1, 1, 1, 2],
        "movieId": [10, 11, 12, 10],
        "rank": [1, 2, 3, 1],
        "score": [4.9, 4.5, 4.1, 3.2],
    })
    movies = pd.DataFrame({
        "movieId": [10, 11, 12],
        "title": ["Ma trận (1999)", "Kẻ huỷ diệt (1984)", "Tình yêu (2001)"],
        "genres": ["Action|Sci-Fi", "Action|Sci-Fi", "Romance"],
    })
    with sqlite3.connect(db) as conn:
        recs.to_sql("recommendations", conn, if_exists="replace", index=False)
        movies.to_sql("movies", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX idx_u ON recommendations(userId)")

    # Phim 10 và 11 gần nhau; phim 12 nằm hướng khác
    factors = np.array([[1.0, 0.0], [0.99, 0.1], [0.0, 1.0]], dtype=np.float32)
    factors_path = tmp_path / "item_factors.npy"
    np.save(factors_path, factors)

    index_path = tmp_path / "item_index.parquet"
    pd.DataFrame({"movieId": [10, 11, 12]}).to_parquet(index_path, index=False)

    return db, factors_path, index_path


@pytest.fixture
def client(store_paths, monkeypatch):
    db, factors_path, index_path = store_paths
    monkeypatch.setenv("RECS_SQLITE", str(db))
    monkeypatch.setenv("ITEM_FACTORS_NPY", str(factors_path))
    monkeypatch.setenv("ITEM_INDEX_PARQUET", str(index_path))
    import importlib

    from serving import api

    importlib.reload(api)
    return TestClient(api.app)


def test_health_returns_ok(client):
    assert client.get("/health").json()["status"] == "ok"


def test_recommendations_are_returned_in_rank_order(client):
    body = client.get("/api/users/1/recommendations?k=3").json()

    assert [item["movieId"] for item in body["items"]] == [10, 11, 12]
    assert body["items"][0]["title"] == "Ma trận (1999)"


def test_recommendations_respect_k(client):
    body = client.get("/api/users/1/recommendations?k=2").json()
    assert len(body["items"]) == 2


def test_unknown_user_returns_404(client):
    assert client.get("/api/users/9999/recommendations").status_code == 404


def test_similar_movies_ranked_by_cosine(client):
    body = client.get("/api/movies/10/similar?k=2").json()

    # Phim 11 gần phim 10 hơn phim 12; phim 10 không tự gợi ý chính nó
    assert body["items"][0]["movieId"] == 11
    assert 10 not in [item["movieId"] for item in body["items"]]


def test_search_is_case_insensitive_substring(client):
    body = client.get("/api/movies/search?q=ma tr").json()
    assert 10 in [item["movieId"] for item in body["items"]]


def test_serving_does_not_import_pyspark(store_paths):
    """Kiểm tra trong TIẾN TRÌNH CON.

    Không kiểm tra được bằng `"pyspark" not in sys.modules` ở đây: conftest.py
    đã nạp pyspark vào tiến trình test từ trước, nên phép kiểm tra đó luôn sai
    bất kể serving/ có sạch hay không.

    Ràng buộc này quan trọng vì image Dockerfile.app không có Java — một câu
    `import pyspark` lọt vào serving/ sẽ làm container chết lúc khởi động.
    """
    import subprocess
    import sys
    import textwrap

    db, factors_path, index_path = store_paths
    script = textwrap.dedent(f"""
        import os, sys
        os.environ["RECS_SQLITE"] = {str(db)!r}
        os.environ["ITEM_FACTORS_NPY"] = {str(factors_path)!r}
        os.environ["ITEM_INDEX_PARQUET"] = {str(index_path)!r}
        import serving.api
        sys.exit(1 if "pyspark" in sys.modules else 0)
    """)
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)

    assert result.returncode == 0, f"serving/ đã kéo theo pyspark:\n{result.stderr}"
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_api.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'serving.store'`

- [ ] **Step 3: Viết `serving/store.py`**

```python
"""Truy cập dữ liệu cho tầng serving.

KHÔNG import pyspark. Tầng này chỉ đọc kết quả mà tầng batch đã ghi ra:
mỗi lần khởi tạo SparkSession tốn 10-20 giây, đặt trong đường xử lý request
sẽ làm demo trông như treo.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


class Store:
    def __init__(self, db_path: Path, factors_path: Path, index_path: Path):
        self.db_path = Path(db_path)
        self._conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

        matrix = np.load(factors_path).astype(np.float32)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        self._factors = matrix / norms          # chuẩn hoá sẵn -> cosine là một phép nhân

        movie_ids = pd.read_parquet(index_path)["movieId"].to_numpy()
        self._movie_ids = movie_ids
        self._row_of = {int(mid): row for row, mid in enumerate(movie_ids)}

    def _movie_rows(self, movie_ids):
        if not movie_ids:
            return {}
        placeholders = ",".join("?" for _ in movie_ids)
        rows = self._conn.execute(
            f"SELECT movieId, title, genres FROM movies WHERE movieId IN ({placeholders})",
            list(movie_ids),
        ).fetchall()
        return {row["movieId"]: dict(row) for row in rows}

    def recommendations(self, user_id: int, k: int = 10) -> list:
        rows = self._conn.execute(
            "SELECT movieId, rank, score FROM recommendations "
            "WHERE userId = ? ORDER BY rank LIMIT ?",
            (user_id, k),
        ).fetchall()
        meta = self._movie_rows([row["movieId"] for row in rows])
        return [
            {
                "movieId": row["movieId"],
                "rank": row["rank"],
                "score": round(row["score"], 4),
                "title": meta.get(row["movieId"], {}).get("title", "?"),
                "genres": meta.get(row["movieId"], {}).get("genres", ""),
            }
            for row in rows
        ]

    def history(self, user_id: int, k: int = 10) -> list:
        """Phim user đã chấm điểm cao nhất.

        Đọc từ bảng ratings_sample mà Task 11 ghi kèm (mẫu rating của user),
        để giao diện đặt lịch sử cạnh gợi ý cho người xem tự đối chiếu.
        """
        rows = self._conn.execute(
            "SELECT movieId, rating FROM ratings_sample "
            "WHERE userId = ? ORDER BY rating DESC, movieId LIMIT ?",
            (user_id, k),
        ).fetchall()
        meta = self._movie_rows([row["movieId"] for row in rows])
        return [
            {
                "movieId": row["movieId"],
                "rating": row["rating"],
                "title": meta.get(row["movieId"], {}).get("title", "?"),
                "genres": meta.get(row["movieId"], {}).get("genres", ""),
            }
            for row in rows
        ]

    def similar(self, movie_id: int, k: int = 10) -> list:
        """Cosine similarity trên item factors đã chuẩn hoá.

        62.000 x rank chỉ vài chục MB, nên nhân một vector với cả ma trận mất
        vài mili giây — không cần tiền tính toán ma trận 62.000 x 62.000.
        """
        row = self._row_of.get(int(movie_id))
        if row is None:
            return []
        scores = self._factors @ self._factors[row]
        scores[row] = -np.inf                     # không tự gợi ý chính nó
        top = np.argsort(-scores)[:k]
        movie_ids = [int(self._movie_ids[i]) for i in top]
        meta = self._movie_rows(movie_ids)
        return [
            {
                "movieId": mid,
                "similarity": round(float(scores[i]), 4),
                "title": meta.get(mid, {}).get("title", "?"),
                "genres": meta.get(mid, {}).get("genres", ""),
            }
            for i, mid in zip(top, movie_ids)
        ]

    def search(self, query: str, limit: int = 10) -> list:
        rows = self._conn.execute(
            "SELECT movieId, title, genres FROM movies WHERE title LIKE ? "
            "ORDER BY LENGTH(title) LIMIT ?",
            (f"%{query}%", limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def stats(self) -> dict:
        n_users = self._conn.execute(
            "SELECT COUNT(DISTINCT userId) FROM recommendations"
        ).fetchone()[0]
        n_movies = self._conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        return {
            "users_with_recommendations": n_users,
            "movies_in_catalog": n_movies,
            "factor_rank": int(self._factors.shape[1]),
        }
```

Lưu ý: `history()` đọc bảng `ratings_sample`. Task 11 bổ sung bảng này vào `export_recs.py`; tới lúc đó test của Task 9 cho `history` chưa tồn tại nên không có gì hỏng.

- [ ] **Step 4: Viết `serving/api.py`**

```python
"""FastAPI phục vụ gợi ý. Không import pyspark."""
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from serving.store import Store

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/data"))
RECS_SQLITE = Path(os.environ.get("RECS_SQLITE", DATA_ROOT / "output" / "recs.sqlite"))
ITEM_FACTORS_NPY = Path(os.environ.get("ITEM_FACTORS_NPY", DATA_ROOT / "output" / "item_factors.npy"))
ITEM_INDEX_PARQUET = Path(os.environ.get("ITEM_INDEX_PARQUET", DATA_ROOT / "output" / "item_index.parquet"))
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="MovieLens ALS Recommender")
store = Store(RECS_SQLITE, ITEM_FACTORS_NPY, ITEM_INDEX_PARQUET)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/users/{user_id}/recommendations")
def recommendations(user_id: int, k: int = Query(10, ge=1, le=20)):
    items = store.recommendations(user_id, k)
    if not items:
        raise HTTPException(status_code=404, detail=f"Không có gợi ý cho user {user_id}")
    return {"userId": user_id, "items": items}


@app.get("/api/users/{user_id}/history")
def history(user_id: int, k: int = Query(10, ge=1, le=50)):
    return {"userId": user_id, "items": store.history(user_id, k)}


@app.get("/api/movies/{movie_id}/similar")
def similar(movie_id: int, k: int = Query(10, ge=1, le=50)):
    items = store.similar(movie_id, k)
    if not items:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy phim {movie_id}")
    return {"movieId": movie_id, "items": items}


@app.get("/api/movies/search")
def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)):
    return {"query": q, "items": store.search(q, limit)}


@app.get("/api/stats")
def stats():
    return store.stats()


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    def index():
        return FileResponse(STATIC_DIR / "index.html")
```

- [ ] **Step 5: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_api.py`
Expected: 7 PASS

- [ ] **Step 6: Commit**

```bash
git add serving/__init__.py serving/store.py serving/api.py tests/test_api.py
git commit -m "feat: API FastAPI phục vụ gợi ý, không phụ thuộc Spark"
```

---

## Task 10: Giao diện demo

**Files:**
- Create: `serving/static/index.html`

**Interfaces:**
- Consumes: các endpoint của Task 9
- Produces: không có API mới

- [ ] **Step 1: Viết `serving/static/index.html`**

Một file duy nhất, HTML + CSS + JavaScript thuần, **không có bước build**. Yêu cầu bố cục:

- Ô nhập `userId` và nút "Xem gợi ý".
- **Hai cột cạnh nhau:** trái là *Lịch sử xem* (gọi `/api/users/{id}/history`), phải là *Gợi ý cho bạn* (gọi `/api/users/{id}/recommendations`). Đặt cạnh nhau là chủ ý — người chấm tự đối chiếu được sở thích và gợi ý, thuyết phục hơn mọi con số RMSE.
- Mỗi phim hiển thị tên, thể loại, và điểm (rating đã chấm hoặc score dự đoán).
- Bấm vào một phim ở cột gợi ý → gọi `/api/movies/{id}/similar` và hiện danh sách "Phim tương tự" bên dưới.
- Ô tìm phim gọi `/api/movies/search`.
- Panel nhỏ ở đầu trang gọi `/api/stats` hiển thị số user, số phim, rank của mô hình.
- Xử lý lỗi 404: hiện "Không có gợi ý cho user này" thay vì để trang trắng.
- Responsive: dưới 700px thì hai cột xếp dọc.

```html
<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Gợi ý phim — MovieLens ALS</title>
  <style>
    :root { --bg:#faf9f7; --fg:#1a1a1a; --muted:#6b6b6b; --line:#e2e0dc; --accent:#2f6f4f; }
    * { box-sizing: border-box; }
    body { margin:0; padding:24px; font:15px/1.5 system-ui, sans-serif; background:var(--bg); color:var(--fg); }
    .wrap { max-width:1000px; margin:0 auto; }
    h1 { font-size:22px; margin:0 0 4px; }
    .stats { color:var(--muted); font-size:13px; margin-bottom:20px; }
    .controls { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:20px; }
    input, button { font:inherit; padding:8px 12px; border:1px solid var(--line); border-radius:6px; }
    button { background:var(--accent); color:#fff; border-color:var(--accent); cursor:pointer; }
    .cols { display:grid; grid-template-columns:1fr 1fr; gap:20px; }
    @media (max-width:700px) { .cols { grid-template-columns:1fr; } }
    .panel { background:#fff; border:1px solid var(--line); border-radius:8px; padding:14px; }
    .panel h2 { font-size:14px; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); margin:0 0 10px; }
    .movie { padding:8px 0; border-bottom:1px solid var(--line); cursor:pointer; }
    .movie:last-child { border-bottom:none; }
    .movie .title { font-weight:600; }
    .movie .meta { color:var(--muted); font-size:13px; }
    .empty { color:var(--muted); font-style:italic; }
  </style>
</head>
<body>
<div class="wrap">
  <h1>Gợi ý phim — MovieLens ALS</h1>
  <div class="stats" id="stats">Đang tải…</div>

  <div class="controls">
    <input id="userId" type="number" min="1" value="1" placeholder="userId">
    <button onclick="loadUser()">Xem gợi ý</button>
    <input id="q" placeholder="Tìm phim…" oninput="doSearch(this.value)">
  </div>

  <div class="cols">
    <div class="panel"><h2>Lịch sử xem</h2><div id="history" class="empty">—</div></div>
    <div class="panel"><h2>Gợi ý cho bạn</h2><div id="recs" class="empty">—</div></div>
  </div>

  <div class="panel" style="margin-top:20px"><h2>Phim tương tự</h2><div id="similar" class="empty">Bấm vào một phim để xem</div></div>
</div>

<script>
const $ = id => document.getElementById(id);

function renderMovies(el, items, scoreKey, onClick) {
  if (!items || !items.length) { el.className = 'empty'; el.textContent = 'Không có dữ liệu'; return; }
  el.className = '';
  el.innerHTML = '';
  for (const m of items) {
    const div = document.createElement('div');
    div.className = 'movie';
    const score = m[scoreKey] !== undefined ? ` · ${Number(m[scoreKey]).toFixed(2)}` : '';
    div.innerHTML = `<div class="title">${m.title}</div><div class="meta">${m.genres || ''}${score}</div>`;
    if (onClick) div.onclick = () => onClick(m.movieId);
    el.appendChild(div);
  }
}

async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
  return res.json();
}

async function loadUser() {
  const id = $('userId').value;
  try {
    const recs = await getJSON(`/api/users/${id}/recommendations?k=10`);
    renderMovies($('recs'), recs.items, 'score', loadSimilar);
  } catch (e) { $('recs').className = 'empty'; $('recs').textContent = e.message; }
  try {
    const hist = await getJSON(`/api/users/${id}/history?k=10`);
    renderMovies($('history'), hist.items, 'rating', loadSimilar);
  } catch (e) { $('history').className = 'empty'; $('history').textContent = e.message; }
}

async function loadSimilar(movieId) {
  try {
    const data = await getJSON(`/api/movies/${movieId}/similar?k=10`);
    renderMovies($('similar'), data.items, 'similarity', loadSimilar);
  } catch (e) { $('similar').className = 'empty'; $('similar').textContent = e.message; }
}

let searchTimer;
function doSearch(q) {
  clearTimeout(searchTimer);
  if (!q) return;
  searchTimer = setTimeout(async () => {
    const data = await getJSON(`/api/movies/search?q=${encodeURIComponent(q)}`);
    renderMovies($('similar'), data.items, null, loadSimilar);
  }, 250);
}

getJSON('/api/stats').then(s => {
  $('stats').textContent = `${s.users_with_recommendations.toLocaleString('vi')} user · ${s.movies_in_catalog.toLocaleString('vi')} phim · rank = ${s.factor_rank}`;
}).catch(() => { $('stats').textContent = 'Chưa có dữ liệu — hãy chạy pipeline trước.'; });

loadUser();
</script>
</body>
</html>
```

- [ ] **Step 2: Khởi động lại app và kiểm tra bằng mắt**

```bash
docker compose -f docker/docker-compose.yml restart app
```

Mở http://localhost:8000. Expected: panel stats hiện số liệu; nhập userId `1` thấy hai cột có dữ liệu; bấm một phim thấy danh sách "Phim tương tự" xuất hiện.

- [ ] **Step 3: Commit**

```bash
git add serving/static/index.html
git commit -m "feat: giao diện demo đặt lịch sử xem cạnh gợi ý"
```

---

## Task 11: Chạy pipeline và thí nghiệm mở rộng

**Files:**
- Create: `scripts/run_pipeline.sh`, `scripts/scaling_experiment.sh`
- Test: `tests/test_pipeline_integration.py`

**Interfaces:**
- Consumes: mọi job từ Task 2, 6, 7, 8
- Produces: `report/results/scaling.csv` với các cột `n_workers`, `total_cores`, `run`, `seconds`

- [ ] **Step 1: Viết integration test thất bại `tests/test_pipeline_integration.py`**

Ba job nối vào nhau qua hệ thống file. Unit test của từng task không bắt được lỗi ở chỗ nối — ví dụ Job 2 ghi cột tên `score` mà Job 4 lại đọc `rating`.

```python
"""Chạy trọn pipeline trên dữ liệu tổng hợp nhỏ, kiểm tra các job nối được."""
import sqlite3

import numpy as np
import pytest

from src.common.split import add_split_column
from src.jobs.evaluate import ranking_metrics, relevant_items
from src.jobs.export_recs import top_rated_per_user, write_sqlite
from src.jobs.train_als import build_als, evaluate_rmse
from pyspark.sql import functions as F


@pytest.fixture
def ratings(spark):
    """40 user x 20 phim, hai cụm sở thích, timestamp tăng dần."""
    rows = []
    for user in range(1, 41):
        cluster = 0 if user <= 20 else 1
        for movie in range(1, 21):
            liked = (movie <= 10) == (cluster == 0)
            rows.append((user, movie, 5.0 if liked else 1.5, 1_000 + user * 100 + movie))
    return spark.createDataFrame(
        rows, "userId int, movieId int, rating double, timestamp long"
    )


def test_pipeline_produces_usable_recommendations(spark, ratings, tmp_path):
    # --- Job 2 tương đương: chia tập ---
    split = add_split_column(ratings)
    train = split.filter("split IN ('train','val')").select("userId", "movieId", "rating")
    test = split.filter("split = 'test'").select("userId", "movieId", "rating")
    assert train.count() > 0 and test.count() > 0

    # --- Job 2: huấn luyện ---
    model = build_als(rank=8, reg_param=0.05, max_iter=10).fit(train)
    rmse = evaluate_rmse(model, test)
    assert not np.isnan(rmse), "RMSE là NaN — coldStartStrategy chưa được đặt"
    assert rmse < 2.0

    # --- Job 3: đánh giá ---
    actual = relevant_items(test, threshold=4.0)
    recs = model.recommendForUserSubset(actual.select("userId"), 5).select(
        "userId", F.col("recommendations.movieId").alias("items")
    )
    metrics = ranking_metrics(recs, actual, k=5)
    assert 0.0 <= metrics["ndcg_at_k"] <= 1.0

    # --- Job 4: xuất sang SQLite ---
    recs_pdf = (
        model.recommendForAllUsers(5)
        .select("userId", F.posexplode("recommendations").alias("pos", "rec"))
        .select(
            "userId",
            F.col("rec.movieId").alias("movieId"),
            (F.col("pos") + 1).alias("rank"),
            F.col("rec.rating").cast("double").alias("score"),
        )
        .toPandas()
    )
    movies_pdf = (
        ratings.select("movieId").distinct()
        .withColumn("title", F.concat(F.lit("Phim "), F.col("movieId")))
        .withColumn("genres", F.lit("Drama"))
        .toPandas()
    )
    sample_pdf = top_rated_per_user(ratings, n=5).toPandas()

    db = tmp_path / "recs.sqlite"
    write_sqlite(recs_pdf, movies_pdf, sample_pdf, db)

    # --- Kiểm tra tầng serving đọc được ---
    with sqlite3.connect(db) as conn:
        n_recs = conn.execute(
            "SELECT COUNT(*) FROM recommendations WHERE userId = 1"
        ).fetchone()[0]
        n_history = conn.execute(
            "SELECT COUNT(*) FROM ratings_sample WHERE userId = 1"
        ).fetchone()[0]
    assert n_recs == 5
    assert n_history == 5
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_pipeline_integration.py`
Expected: FAIL — `ImportError: cannot import name 'top_rated_per_user'` nếu Task 8 chưa xong; nếu Task 8 đã xong thì test này PASS ngay, và đó là kết quả hợp lệ (nó là lưới an toàn cho các thay đổi sau).

- [ ] **Step 3: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_pipeline_integration.py`
Expected: 1 PASS, dưới 60 giây.

- [ ] **Step 4: Viết `scripts/run_pipeline.sh`**

```bash
#!/usr/bin/env bash
# Chạy toàn bộ pipeline batch theo thứ tự.
# Dùng: ./scripts/run_pipeline.sh [ml-25m|ml-latest-small]
set -euo pipefail

DATASET="${1:-ml-25m}"
COMPOSE="docker compose -f $(dirname "$0")/../docker/docker-compose.yml"
SUBMIT="/opt/spark/bin/spark-submit --master spark://spark-master:7077"

for job in ingest train_als evaluate export_recs; do
  echo ""
  echo "=============================================="
  echo "  Đang chạy: $job  (dataset=$DATASET)"
  echo "=============================================="
  $COMPOSE exec -T spark-master \
    env DATASET="$DATASET" SPARK_MASTER_URL=spark://spark-master:7077 \
    $SUBMIT "/opt/app/src/jobs/${job}.py"
done

echo ""
echo "Pipeline xong. Mở http://localhost:8000 để xem demo."
```

- [ ] **Step 5: Viết `scripts/scaling_experiment.sh`**

```bash
#!/usr/bin/env bash
# Đo thời gian huấn luyện ALS theo số worker.
#
# Số core mỗi worker cố định ở 2 để biến duy nhất thay đổi là SỐ LƯỢNG WORKER.
# Nếu vừa đổi số worker vừa đổi core mỗi worker thì không quy kết được kết quả
# cho yếu tố nào.
set -euo pipefail

DATASET="${1:-ml-25m}"
RUNS=3
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $ROOT/docker/docker-compose.yml"
OUT="$ROOT/report/results/scaling.csv"

mkdir -p "$(dirname "$OUT")"
echo "n_workers,total_cores,run,seconds" > "$OUT"

for n in 1 2 4; do
  echo "--- Cấu hình $n worker x 2 core ---"
  SPARK_WORKER_REPLICAS=$n SPARK_WORKER_CORES=2 SPARK_WORKER_MEMORY=2g \
    $COMPOSE up -d --scale spark-worker=$n spark-worker
  sleep 15   # chờ worker đăng ký với master

  for run in $(seq 1 $RUNS); do
    start=$(date +%s.%N)
    $COMPOSE exec -T spark-master \
      env DATASET="$DATASET" SPARK_MASTER_URL=spark://spark-master:7077 \
      /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      /opt/app/src/jobs/train_als.py > /dev/null
    end=$(date +%s.%N)
    seconds=$(echo "$end - $start" | bc)
    echo "$n,$((n * 2)),$run,$seconds" >> "$OUT"
    echo "  lần $run: ${seconds}s"
  done
done

echo "Ghi kết quả: $OUT"
```

`sleep 15` là cần thiết: `docker compose up` trả về ngay khi container khởi động, nhưng worker mất vài giây mới đăng ký xong với master. Chạy job sớm hơn sẽ dùng thiếu worker và làm hỏng phép đo.

- [ ] **Step 6: Chạy pipeline đầy đủ trên ml-25m**

```bash
./scripts/download_data.sh ml-25m
./scripts/run_pipeline.sh ml-25m
```

Expected: cả 4 job xong không lỗi. Nếu executor bị OOM-kill, xem mục Rủi ro trong spec: tăng `.wslconfig` lên 12 GB, hoặc bỏ `rank=100` khỏi `config.ALS_RANKS`.

- [ ] **Step 7: Chụp Spark UI**

Trong lúc `train_als` đang chạy, mở http://localhost:4040 và lưu ảnh vào `report/figures/`: tab **Jobs** (DAG), tab **Stages** (timeline), tab **Executors**.

- [ ] **Step 8: Chạy thí nghiệm scale**

```bash
./scripts/scaling_experiment.sh ml-25m
```

Expected: `report/results/scaling.csv` có 9 dòng (3 cấu hình × 3 lần).

- [ ] **Step 9: Khôi phục cấu hình mặc định và commit**

```bash
docker compose -f docker/docker-compose.yml up -d --scale spark-worker=2 spark-worker
git add tests/test_pipeline_integration.py scripts/run_pipeline.sh scripts/scaling_experiment.sh report/results/
git commit -m "feat: integration test, script chạy pipeline và thí nghiệm đo scale"
```

---

## Task 12: Sinh biểu đồ cho báo cáo

**Files:**
- Create: `report/make_figures.py`
- Test: `tests/test_make_figures.py`

**Interfaces:**
- Consumes: `report/results/tuning.csv`, `metrics.csv`, `scaling.csv`
- Produces:
  - `plot_tuning(tuning_csv: Path, out_png: Path) -> None`
  - `plot_scaling(scaling_csv: Path, out_png: Path) -> None`
  - `plot_baseline_comparison(metrics_csv: Path, out_png: Path) -> None`

- [ ] **Step 1: Viết test thất bại `tests/test_make_figures.py`**

```python
import pandas as pd

from report.make_figures import plot_baseline_comparison, plot_scaling, plot_tuning


def test_plot_tuning_writes_png(tmp_path):
    csv = tmp_path / "tuning.csv"
    pd.DataFrame({
        "rank": [10, 50, 10, 50],
        "regParam": [0.1, 0.1, 0.2, 0.2],
        "maxIter": [10] * 4,
        "rmse": [0.85, 0.82, 0.88, 0.84],
        "fit_seconds": [10, 20, 10, 20],
    }).to_csv(csv, index=False)
    out = tmp_path / "tuning.png"

    plot_tuning(csv, out)

    assert out.exists() and out.stat().st_size > 0


def test_plot_scaling_writes_png(tmp_path):
    csv = tmp_path / "scaling.csv"
    pd.DataFrame({
        "n_workers": [1, 1, 2, 2, 4, 4],
        "total_cores": [2, 2, 4, 4, 8, 8],
        "run": [1, 2, 1, 2, 1, 2],
        "seconds": [400, 410, 250, 245, 180, 185],
    }).to_csv(csv, index=False)
    out = tmp_path / "scaling.png"

    plot_scaling(csv, out)

    assert out.exists() and out.stat().st_size > 0


def test_plot_baseline_comparison_writes_png(tmp_path):
    csv = tmp_path / "metrics.csv"
    pd.DataFrame({
        "model": ["als", "global_mean", "item_mean", "popularity"],
        "rmse": [0.81, 1.05, 0.95, None],
        "mae": [0.62, 0.84, 0.74, None],
        "precision_at_k": [0.09, None, None, 0.06],
        "recall_at_k": [0.11, None, None, 0.07],
        "ndcg_at_k": [0.13, None, None, 0.08],
        "coverage": [0.42, None, None, 0.001],
    }).to_csv(csv, index=False)
    out = tmp_path / "baselines.png"

    plot_baseline_comparison(csv, out)

    assert out.exists() and out.stat().st_size > 0
```

- [ ] **Step 2: Chạy test, xác nhận thất bại**

Run: `./scripts/test.sh tests/test_make_figures.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'report.make_figures'`

- [ ] **Step 3: Viết `report/make_figures.py`**

```python
"""Sinh biểu đồ cho báo cáo từ các file CSV kết quả.

Dùng: python report/make_figures.py
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")           # không có màn hình trong container
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd              # noqa: E402

RESULTS_DIR = Path(__file__).parent / "results"
FIGURES_DIR = Path(__file__).parent / "figures"

PALETTE = ["#2f6f4f", "#b5651d", "#3f5f8f", "#8f3f5f"]


def plot_tuning(tuning_csv: Path, out_png: Path) -> None:
    """RMSE theo rank, mỗi regParam một đường."""
    df = pd.read_csv(tuning_csv)
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for color, (reg, group) in zip(PALETTE, df.groupby("regParam")):
        group = group.sort_values("rank")
        ax.plot(group["rank"], group["rmse"], marker="o", color=color, label=f"regParam = {reg}")
    ax.set_xlabel("rank (số chiều ẩn)")
    ax.set_ylabel("RMSE trên tập validation")
    ax.set_title("Ảnh hưởng của rank và regParam tới RMSE")
    ax.legend(frameon=False)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def plot_scaling(scaling_csv: Path, out_png: Path) -> None:
    """Speedup theo số core, kèm đường tuyến tính lý tưởng để đối chiếu."""
    df = pd.read_csv(scaling_csv)
    mean = df.groupby("total_cores")["seconds"].mean().sort_index()
    baseline = mean.iloc[0]
    speedup = baseline / mean

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(mean.index, speedup, marker="o", color=PALETTE[0], label="Speedup thực đo")
    ideal = mean.index / mean.index[0]
    ax.plot(mean.index, ideal, linestyle="--", color="#999999", label="Tuyến tính lý tưởng")
    ax.set_xlabel("Tổng số core")
    ax.set_ylabel(f"Speedup (so với {mean.index[0]} core)")
    ax.set_title("Khả năng mở rộng của ALS theo số core")
    ax.legend(frameon=False)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def plot_baseline_comparison(metrics_csv: Path, out_png: Path) -> None:
    """So sánh ALS với các baseline trên metrics xếp hạng."""
    df = pd.read_csv(metrics_csv).set_index("model")
    columns = ["precision_at_k", "recall_at_k", "ndcg_at_k"]
    subset = df[columns].dropna(how="all")

    fig, ax = plt.subplots(figsize=(7, 4.5))
    n_models = len(subset)
    width = 0.8 / n_models
    positions = range(len(columns))
    for offset, (color, (model, row)) in enumerate(zip(PALETTE, subset.iterrows())):
        xs = [p + offset * width for p in positions]
        ax.bar(xs, row[columns].values, width=width, color=color, label=model)

    ax.set_xticks([p + width * (n_models - 1) / 2 for p in positions])
    ax.set_xticklabels(["Precision@10", "Recall@10", "NDCG@10"])
    ax.set_ylabel("Giá trị")
    ax.set_title("ALS so với baseline (ngưỡng liên quan ≥ 4.0 sao)")
    ax.legend(frameon=False)
    ax.grid(alpha=0.25, axis="y")
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def main() -> None:
    jobs = [
        (plot_tuning, "tuning.csv", "tuning.png"),
        (plot_scaling, "scaling.csv", "scaling.png"),
        (plot_baseline_comparison, "metrics.csv", "baselines.png"),
    ]
    for plot_fn, source, target in jobs:
        csv_path = RESULTS_DIR / source
        if not csv_path.exists():
            print(f"Bỏ qua {target}: chưa có {csv_path}")
            continue
        plot_fn(csv_path, FIGURES_DIR / target)
        print(f"Đã tạo {FIGURES_DIR / target}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Chạy test, xác nhận PASS**

Run: `./scripts/test.sh tests/test_make_figures.py`
Expected: 3 PASS

- [ ] **Step 5: Sinh biểu đồ thật**

```bash
docker compose -f docker/docker-compose.yml exec -T spark-master \
  python /opt/app/report/make_figures.py
```

Expected: tạo 3 file PNG trong `report/figures/`. Mở xem và kiểm tra trục, nhãn, chú giải đọc được.

- [ ] **Step 6: Chạy toàn bộ test lần cuối**

Run: `./scripts/test.sh`
Expected: toàn bộ ~45 test PASS.

- [ ] **Step 7: Commit**

```bash
git add report/make_figures.py tests/test_make_figures.py
git commit -m "feat: sinh biểu đồ tuning, scaling và so sánh baseline"
```

---

## Đối chiếu với spec

| Mục spec | Task thực hiện |
|---|---|
| §3.1–3.2 Docker, image, mount | Task 1 |
| §3.3 `.wslconfig` | Task 1 Step 13 (README) |
| §3.4 Cấu trúc thư mục | Task 1 |
| §4.1 Dataset | Task 1 Step 8 (`download_data.sh`) |
| §4.2 Ingest, schema tường minh, tránh small-files | Task 2 |
| §4.3 Chia 70/15/15 theo thời gian, loại user ít rating | Task 3 |
| §5.1 ALS, grid search, coldStart, checkpoint | Task 6 |
| §5.2 RMSE/MAE, ranking metrics, ngưỡng 4.0, coverage | Task 4 + Task 7 |
| §5.2 Ba baseline | Task 5 + Task 7 |
| §5.3 Export SQLite + item factors | Task 8 |
| §6.1 API, không đụng Spark | Task 9 |
| §6.2 Giao diện hai cột, không build step | Task 10 |
| §7 Unit test | Task 2–9 |
| §7 Integration test trọn pipeline | Task 11 Step 1–3 |
| §7 Test API | Task 9 |
| §8 Thí nghiệm mở rộng | Task 11 Step 5, 8 |
| §9.1 Repo + README | Task 1 Step 13 |
| §9.2 Biểu đồ | Task 12 |
| §9.3 Ảnh Spark UI | Task 11 Step 7 |
| §10 Lộ trình: làm bộ nhỏ trước | Task 2, 6, 7, 8 đều chạy `ml-latest-small` trước; `ml-25m` chỉ ở Task 11 Step 6 |
