# Kế Hoạch Triển Khai HDFS Cho Tầng Lake

> **Dành cho agentic worker:** REQUIRED SUB-SKILL: dùng `superpowers:subagent-driven-development` (khuyến nghị) hoặc `superpowers:executing-plans` để thực thi từng task. Các bước dùng cú pháp checkbox (`- [ ]`) để theo dõi tiến độ.

**Goal:** Đưa tầng lake Parquet từ Docker bind mount lên cụm HDFS (1 NameNode + 2 DataNode, replication=2), giữ nguyên tầng raw và tầng serving ở local, sao cho pipeline chạy được ở **cả hai chế độ** mà không phải sửa code.

**Architecture:** Thêm `LAKE_ROOT` làm biến môi trường kiểu **chuỗi** (không phải `Path`, vì `Path` bóp `hdfs://` thành `hdfs:/`). Khi `HDFS_NAMENODE` được đặt, `src/session.py` thêm `spark.hadoop.fs.defaultFS` vào SparkSession. Mọi thao tác hệ thống file trong `src/jobs/ingest.py` vốn dùng `pathlib` được thay bằng Hadoop FileSystem API qua py4j gateway, nên chạy đúng trên cả `file://` lẫn `hdfs://`. Không đặt biến môi trường thì hành vi **giống hệt hiện tại**.

**Tech Stack:** Apache Hadoop 3.3.6 (image `apache/hadoop:3.3.6`), Apache Spark 3.5.3 (đã đóng gói sẵn Hadoop client 3.3.x — không cần thêm jar), Docker Compose, PySpark, pytest.

**Spec:** `docs/superpowers/specs/2026-09-18-movielens-als-recsys-design.md` (thiết kế gốc) + mục "Bối Cảnh & Quyết Định Thiết Kế" ngay bên dưới (đặc tả riêng của phần mở rộng này — đây là một extension, không phải subsystem mới, nên spec đi kèm trong plan).

---

## Bối Cảnh & Quyết Định Thiết Kế

### Vấn đề

`report/baocao_kientruc_pipeline.html` mô tả kiến trúc HDFS với NameNode/DataNode và Replication Factor = 3, nhưng mã nguồn **không có một dòng nào** liên quan tới HDFS (xác minh bằng `grep -rni "hdfs|namenode|datanode|yarn" src/ serving/ docker/ scripts/` → không khớp dòng nào). Hiện tại mọi container mount chung một thư mục host qua `../data:/opt/data`.

### HDFS trên một máy đơn cho gì và không cho gì

| HDFS hứa hẹn | Thực tế trên máy này |
|---|---|
| Chịu lỗi qua replication | **Không có thật** — mọi DataNode nằm trên cùng một ổ SSD vật lý |
| Data locality | **Không có thật** — mọi node đọc cùng một đĩa |
| Mở rộng dung lượng | **Không có thật** — vẫn giới hạn bởi ổ D: |
| Tốc độ | **Chậm hơn** bind mount: ghi qua TCP + nhân bản 2 lần |

Ba lợi ích **có thật**, và là lý do làm task này:

1. Chứng minh hiểu kiến trúc — mở được NameNode UI, chỉ ra block mapping và replica placement bằng `hdfs fsck`.
2. Xác nhận mã nguồn tương thích HDFS — đổi sang cụm nhiều máy vật lý là chạy ngay.
3. Đo được **chi phí của lớp trừu tượng** — một kết quả âm có giá trị phân tích cao.

### Quyết định phạm vi: chỉ tầng lake

| Thư mục | Lên HDFS? | Lý do |
|---|---|---|
| `data/lake/*.parquet` | **CÓ** | Đầu ra Job 1, đầu vào Job 2–4. Parquet, đọc song song bởi executor — đúng kiểu truy cập HDFS tối ưu |
| `data/raw/*.csv` | KHÔNG | Chỉ đọc một lần ở Job 1; `hdfs dfs -put` 1,1 GB tốn thời gian mà không chứng minh thêm điều gì |
| `data/output/recs.sqlite` | **KHÔNG BAO GIỜ** | SQLite cần random access + file locking. HDFS là write-once-read-many, append-only. **Không thể chạy** |
| `data/output/model/` | KHÔNG | `src/common/eligibility.py` dùng `shutil.copytree` — thư viện Python thuần, không hiểu `hdfs://` |
| `data/checkpoint/` | KHÔNG | Dữ liệu tạm, không có lý do phân tán |

Quyết định này là **kiến trúc có chủ đích**, đáng viết vào báo cáo: mỗi tầng dùng hệ lưu trữ phù hợp với kiểu truy cập của nó, không phải "đẩy hết lên HDFS cho giống công nghiệp".

### Ngân sách RAM (WSL2 = 12 GB)

Hiện tại đã dùng ~10,3 GB:

```
spark-master     Master daemon ~0,5 GB + Driver 2 GB + overhead  ≈ 3,0 GB
spark-worker ×2  Worker daemon ~0,5 GB + Executor 3 GB + overhead ≈ 3,5 GB × 2 = 7,0 GB
movielens-api                                                     ≈ 0,3 GB
```

Thêm Hadoop với heap mặc định (NameNode 1 GB + 3 DataNode 1 GB) → ~14,3 GB → **vượt 12 GB, sẽ bị OOM-kill**. Triệu chứng rất khó chẩn đoán: container biến mất im lặng hoặc "Connection reset", không có exception Spark.

Ba biện pháp bắt buộc, dùng đồng thời:

| Biện pháp | Tiết kiệm |
|---|---|
| Ghim heap Hadoop xuống `-Xmx512m` | ~2 GB |
| Dùng **2 DataNode**, `dfs.replication=2` (không phải 3) | ~1 GB |
| Hạ `SPARK_WORKER_MEMORY` 3g → 2g | ~2 GB |

Ngân sách sau khi áp dụng: **~9,7 GB**, còn dư ~2,3 GB.

### Cái bẫy `pathlib`

`Path("hdfs://namenode:9000/x")` cho ra `hdfs:/namenode:9000/x` — **mất một dấu gạch chéo**, Spark không phân giải được URI. Mọi đường dẫn có thể là URI **bắt buộc dùng `str`**, không dùng `Path`.

### Các chỗ trong code sẽ vỡ (đã rà soát bằng grep)

| File:dòng | Vấn đề |
|---|---|
| `src/config.py:10,20,21` | `LAKE_DIR` là `Path`, nối chuỗi bằng `/` |
| `src/jobs/ingest.py:85` | `config.LAKE_DIR.mkdir()` — API của `Path` |
| `src/jobs/ingest.py:100` | `_dir_bytes(config.RATINGS_PARQUET)` — dùng `Path.rglob`, `Path.stat` |
| `tests/test_smoke.py:15` | `assert str(config.RATINGS_PARQUET).startswith(root)` — sai khi lake ở HDFS |

**Không cần sửa:** các lời gọi `spark.read.parquet(str(config.RATINGS_PARQUET))` ở `ingest.py:87,90,93`, `train_als.py:128`, `evaluate.py:94`, `export_recs.py:68-69` — `str()` áp lên một chuỗi là no-op, giữ nguyên để diff nhỏ nhất.

### Không chạy lại ml-25m

Số liệu trong `report/results/` đã tốn ~80 phút cụm để đo và đã commit. Toàn bộ phát triển và nghiệm thu dùng `ml-latest-small`. RAM đã sát trần, chạy `ml-25m` kèm 3 JVM Hadoop có nguy cơ OOM cao.

---

## Global Constraints

Mọi task đều ngầm bao gồm các ràng buộc sau. Giá trị chép nguyên văn từ spec gốc và từ phần Bối Cảnh trên.

- **Python 3.8.10** — image `spark:3.5.3-python3` chạy Python 3.8. KHÔNG dùng: PEP 604 union (`str | None` → dùng `Optional[str]`), builtin generic trong annotation (`list[int]` → `List[int]`), `Path.is_relative_to()`, `str.removeprefix`/`removesuffix`, toán tử `|` hợp nhất dict.
- **Mọi đường dẫn có thể là URI dùng `str`, không dùng `Path`** — `Path` bóp `hdfs://` thành `hdfs:/`.
- **Mặc định phải giữ nguyên hành vi cũ** — không đặt `HDFS_NAMENODE`/`LAKE_ROOT` thì pipeline chạy y hệt hiện tại trên bind mount. Đây là điều kiện để không phải chạy lại `ml-25m`.
- **`dfs.replication=2`** (không phải 3) — chỉ có 2 DataNode; replication > số DataNode làm NameNode báo under-replicated vĩnh viễn.
- **`dfs.blocksize=134217728`** (128 MB) — khớp với `TARGET_FILE_MB = 128` trong `src/jobs/ingest.py:12`.
- **`dfs.permissions.enabled=false`** — container Spark chạy uid 185 (`spark`), container Hadoop chạy user `hadoop`; không tắt kiểm tra quyền sẽ bị `AccessControlException` khi ghi.
- **Heap Hadoop `-Xmx512m`** cho cả NameNode và DataNode — ngân sách RAM.
- **`SPARK_WORKER_MEMORY` mặc định đổi 3g → 2g** — ngân sách RAM.
- **Mọi script bash tự `export MSYS_NO_PATHCONV=1`** — Git Bash trên Windows dịch `/opt/spark/...` thành `C:/Program Files/Git/opt/spark/...` → exit 127. Đường dẫn file compose phải để **tương đối** vì cờ đó lại làm hỏng đường dẫn host tuyệt đối `/d/...`.
- **Dataset phát triển: `ml-latest-small`.** KHÔNG chạy `ml-25m` trong bất kỳ task nào.
- **Không ghi đè `report/results/*.csv`** — đó là số liệu `ml-25m` đã commit. Kết quả so sánh HDFS ghi vào file riêng `report/results/storage_comparison.csv`.
- **Làm trên branch `feat/hdfs`**, không commit thẳng vào `main`.

---

## Cấu Trúc File

| File | Trách nhiệm | Thao tác |
|---|---|---|
| `docker/hadoop/core-site.xml` | Khai báo `fs.defaultFS` cho cụm Hadoop | Tạo |
| `docker/hadoop/hdfs-site.xml` | Replication, blocksize, thư mục dữ liệu, tắt permission | Tạo |
| `docker/docker-compose.yml` | Thêm service `namenode` + `datanode`; hạ RAM worker | Sửa |
| `src/config.py` | `LAKE_ROOT` dạng chuỗi + `is_remote_lake()` | Sửa |
| `src/session.py` | `_apply_hdfs_config()` — đặt `fs.defaultFS` có điều kiện | Sửa |
| `src/jobs/ingest.py` | `_path_bytes()` qua Hadoop FS; bỏ `mkdir` khi lake ở xa | Sửa |
| `scripts/run_pipeline_hdfs.sh` | Chạy pipeline với lake trên HDFS | Tạo |
| `tests/test_config_lake.py` | Test `LAKE_ROOT` và `is_remote_lake()` | Tạo |
| `tests/test_session_hdfs.py` | Test `_apply_hdfs_config()` bằng builder giả | Tạo |
| `tests/test_smoke.py` | Sửa assertion `startswith(root)` | Sửa |
| `tests/test_ingest.py` | Thêm test cho `_path_bytes()` | Sửa |
| `report/results/storage_comparison.csv` | Số liệu so sánh bind mount vs HDFS | Tạo (Task 6) |
| `README.md` | Mục "Chạy với HDFS" | Sửa (Task 6) |

---

## Task 1: Tách `LAKE_ROOT` khỏi `Path` trong config

**Files:**
- Modify: `src/config.py:8-21`
- Modify: `src/jobs/ingest.py:85` (chỉ đổi tên `LAKE_DIR` → `LAKE_ROOT`, giữ suite xanh; Task 3 thêm nhánh HDFS)
- Modify: `tests/test_smoke.py:12-16`
- Test: `tests/test_config_lake.py` (tạo mới)

**Interfaces:**
- Consumes: không có (task đầu tiên)
- Produces:
  - `config.LAKE_ROOT: str` — gốc của lake, mặc định `str(DATA_ROOT / "lake")`, ghi đè bằng biến môi trường `LAKE_ROOT`
  - `config.RATINGS_PARQUET: str` — `LAKE_ROOT + "/ratings.parquet"`
  - `config.MOVIES_PARQUET: str` — `LAKE_ROOT + "/movies.parquet"`
  - `config.is_remote_lake() -> bool` — True khi `LAKE_ROOT` là URI không phải `file://`
  - `config.LAKE_DIR` **bị xoá** — mọi nơi dùng nó phải chuyển sang `LAKE_ROOT`

- [ ] **Step 1: Viết test thất bại**

Tạo `tests/test_config_lake.py`:

```python
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

    Đây là test quan trọng nhất của task: nếu ai đó đổi LAKE_ROOT về Path,
    test này fail ngay với "hdfs:/namenode..." thay vì "hdfs://namenode...".
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

    mkdir và pathlib làm việc được với nó sau khi bỏ tiền tố, nên không coi
    là "xa" — nếu coi là xa, ingest sẽ bỏ qua mkdir và lần ghi đầu tiên vào
    một thư mục chưa tồn tại sẽ hỏng.
    """
    monkeypatch.setenv("LAKE_ROOT", "file:///opt/data/lake")
    importlib.reload(config)
    try:
        assert config.is_remote_lake() is False
    finally:
        importlib.reload(config)
```

- [ ] **Step 2: Chạy test để xác nhận THẤT BẠI**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh tests/test_config_lake.py -v
```

Kỳ vọng: FAIL với `AttributeError: module 'src.config' has no attribute 'LAKE_ROOT'`.

- [ ] **Step 3: Sửa `src/config.py`**

Thay dòng 10 và 20–21. Xoá `LAKE_DIR`, thêm `LAKE_ROOT` và `is_remote_lake()`:

```python
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/data"))
RAW_DIR = DATA_ROOT / "raw"
OUTPUT_DIR = DATA_ROOT / "output"
CHECKPOINT_DIR = DATA_ROOT / "checkpoint"
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/opt/app/report/results"))

# Gốc của lake Parquet. Dùng CHUỖI, không dùng Path: Path("hdfs://namenode:9000")
# cho ra "hdfs:/namenode:9000" (mất một dấu gạch chéo) và Spark không phân giải
# được URI — lỗi âm thầm, không có exception, chỉ báo không tìm thấy đường dẫn.
# Không đặt biến môi trường LAKE_ROOT thì giá trị y hệt DATA_ROOT/"lake" như cũ,
# nên pipeline chạy đúng như trước và không phải đo lại ml-25m.
LAKE_ROOT = os.environ.get("LAKE_ROOT", str(DATA_ROOT / "lake"))

RATINGS_CSV = RAW_DIR / DATASET / "ratings.csv"
MOVIES_CSV = RAW_DIR / DATASET / "movies.csv"
RATINGS_PARQUET = LAKE_ROOT + "/ratings.parquet"
MOVIES_PARQUET = LAKE_ROOT + "/movies.parquet"
```

Thêm hàm ở cuối file, cạnh `is_scaling_run()`:

```python
def is_remote_lake() -> bool:
    """True khi LAKE_ROOT là URI của một hệ thống file KHÔNG phải local.

    Dùng để src/jobs/ingest.py biết có được gọi Path.mkdir() hay không: trên
    HDFS, thư mục do Spark tự tạo khi ghi, và pathlib không hiểu "hdfs://".

    "file://" tuy là URI nhưng vẫn là hệ thống file cục bộ nên trả về False —
    coi nó là xa sẽ làm ingest bỏ qua mkdir và lần ghi đầu tiên vào thư mục
    chưa tồn tại sẽ hỏng.
    """
    return "://" in LAKE_ROOT and not LAKE_ROOT.startswith("file://")
```

- [ ] **Step 4: Sửa assertion trong `tests/test_smoke.py`**

Dòng 12–16 hiện tại khẳng định `RATINGS_PARQUET` nằm dưới `DATA_ROOT`, không còn đúng khi lake ở HDFS. Thay bằng:

```python
def test_config_paths_are_under_data_root():
    # Không dùng Path.is_relative_to: nó chỉ có từ Python 3.9, container Spark chạy 3.8
    root = str(config.DATA_ROOT)
    assert str(config.MODEL_DIR).startswith(root)
    # RATINGS_PARQUET KHÔNG kiểm tra theo DATA_ROOT nữa: nó có thể trỏ sang
    # HDFS qua biến môi trường LAKE_ROOT (xem tests/test_config_lake.py).
    assert config.RATINGS_PARQUET.startswith(config.LAKE_ROOT)
```

- [ ] **Step 5: Chạy test để xác nhận THÀNH CÔNG**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh tests/test_config_lake.py tests/test_smoke.py -v
```

Kỳ vọng: toàn bộ PASS.

- [ ] **Step 6: Chạy TOÀN BỘ suite — tìm chỗ còn dùng `LAKE_DIR`**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh -v
```

Kỳ vọng: `tests/test_ingest.py` FAIL với `AttributeError: module 'src.config' has no attribute 'LAKE_DIR'`. Nguồn gốc là `src/jobs/ingest.py:85`.

- [ ] **Step 7: Sửa tối thiểu `src/jobs/ingest.py:85` để suite xanh trở lại**

Chỉ đổi tên, **chưa** thêm nhánh HDFS (Task 3 làm việc đó). Task này phải kết thúc với suite xanh để reviewer gate được độc lập:

```python
    Path(config.LAKE_ROOT).mkdir(parents=True, exist_ok=True)
```

`Path` đã được import sẵn ở `src/jobs/ingest.py:4`, không cần thêm import.

- [ ] **Step 8: Chạy lại TOÀN BỘ suite**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh -v
```

Kỳ vọng: **toàn bộ PASS** (81 test cũ + 5 test mới = 86). Không được commit khi còn test đỏ.

- [ ] **Step 9: Commit**

```bash
git add src/config.py src/jobs/ingest.py tests/test_config_lake.py tests/test_smoke.py
git commit -m "feat: LAKE_ROOT dạng chuỗi để lake trỏ được sang HDFS

Path bóp 'hdfs://' thành 'hdfs:/' nên mọi đường dẫn có thể là URI phải dùng
str. Không đặt biến môi trường LAKE_ROOT thì giá trị không đổi so với trước.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 2: Đặt `fs.defaultFS` có điều kiện trong session

**Files:**
- Modify: `src/session.py:1-33`
- Test: `tests/test_session_hdfs.py` (tạo mới)

**Interfaces:**
- Consumes: không có
- Produces:
  - `session._apply_hdfs_config(builder)` — nhận một `SparkSession.Builder`, trả về chính nó; thêm `spark.hadoop.fs.defaultFS` khi biến môi trường `HDFS_NAMENODE` được đặt

**Ghi chú thiết kế:** tách thành hàm riêng thay vì nhét thẳng vào chuỗi `.config()` vì `getOrCreate()` trả về session đang tồn tại — test nào gọi `get_spark()` rồi kiểm tra config sẽ **tautological** (pass bất kể code đúng hay sai). Một builder giả cho phép test thật sự kiểm tra hành vi. Lỗi tautological này đã từng xảy ra trong dự án với `test_get_spark_sets_checkpoint_dir`.

- [ ] **Step 1: Viết test thất bại**

Tạo `tests/test_session_hdfs.py`:

```python
"""_apply_hdfs_config phải đặt fs.defaultFS CHỈ KHI HDFS_NAMENODE được đặt.

Không test qua get_spark(): getOrCreate() trả về session đang tồn tại nên
mọi assertion về config sẽ pass bất kể code đúng hay sai (lỗi tautological đã
từng xảy ra trong dự án với test_get_spark_sets_checkpoint_dir). Builder giả
bên dưới ghi lại đúng những lời gọi .config() thật sự diễn ra.
"""
from src import session


class _FakeBuilder:
    """Ghi lại các lời gọi .config(key, value) và trả về chính mình như Builder thật."""

    def __init__(self):
        self.calls = []

    def config(self, key, value):
        self.calls.append((key, value))
        return self


def test_apply_hdfs_config_sets_default_fs_when_env_set(monkeypatch):
    monkeypatch.setenv("HDFS_NAMENODE", "hdfs://namenode:9000")
    builder = _FakeBuilder()

    result = session._apply_hdfs_config(builder)

    assert ("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") in builder.calls
    assert result is builder


def test_apply_hdfs_config_is_noop_when_env_unset(monkeypatch):
    """Không đặt biến thì KHÔNG gọi .config() lần nào.

    Đây là điều kiện để pipeline bind mount chạy y hệt như trước và số liệu
    ml-25m đã đo không phải chạy lại.
    """
    monkeypatch.delenv("HDFS_NAMENODE", raising=False)
    builder = _FakeBuilder()

    session._apply_hdfs_config(builder)

    assert builder.calls == []


def test_apply_hdfs_config_ignores_empty_string(monkeypatch):
    """HDFS_NAMENODE="" (đặt nhưng rỗng) phải coi như không đặt.

    docker compose truyền biến rỗng khi người dùng khai báo biến mà không gán
    giá trị; đặt fs.defaultFS = "" sẽ làm SparkSession hỏng với thông báo khó hiểu.
    """
    monkeypatch.setenv("HDFS_NAMENODE", "")
    builder = _FakeBuilder()

    session._apply_hdfs_config(builder)

    assert builder.calls == []
```

- [ ] **Step 2: Chạy test để xác nhận THẤT BẠI**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh tests/test_session_hdfs.py -v
```

Kỳ vọng: FAIL với `AttributeError: module 'src.session' has no attribute '_apply_hdfs_config'`.

- [ ] **Step 3: Sửa `src/session.py`**

Thêm hàm trước `get_spark`, và gọi nó trong chuỗi builder:

```python
"""Khởi tạo SparkSession dùng chung cho mọi job."""
import os
from typing import Optional

from pyspark.sql import SparkSession

from src import config


def _apply_hdfs_config(builder):
    """Trỏ Spark vào NameNode khi biến môi trường HDFS_NAMENODE được đặt.

    Spark 3.5.3 đã đóng gói sẵn Hadoop client 3.3.x nên đọc/ghi "hdfs://" chạy
    được ngay, không cần thêm jar — chỉ cần cho Spark biết NameNode ở đâu.

    Không đặt biến thì KHÔNG gọi .config() lần nào, để hành vi giống hệt trước
    khi có HDFS: đó là điều kiện để số liệu ml-25m đã đo trên bind mount không
    phải chạy lại.

    Chuỗi rỗng cũng coi như không đặt: docker compose truyền biến rỗng khi người
    dùng khai báo mà không gán giá trị, và fs.defaultFS = "" làm SparkSession
    hỏng với thông báo khó lần ra.
    """
    namenode = os.environ.get("HDFS_NAMENODE")
    if namenode:
        builder = builder.config("spark.hadoop.fs.defaultFS", namenode)
    return builder


def get_spark(app_name: str, master: Optional[str] = None) -> SparkSession:
    """Tạo SparkSession và đặt checkpoint dir.

    Checkpoint dir bắt buộc phải có: ALS lặp nhiều vòng sinh lineage RDD rất
    dài và sẽ ném StackOverflowError nếu không được cắt định kỳ.
    """
    master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "2g"))
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Mặc định Spark là false: checkpoint của ALS bị bỏ lại trên đĩa mãi mãi
        # thay vì bị dọn khi RDD tham chiếu tới nó hết vòng đời. Vô hại ở quy mô
        # ml-latest-small, nhưng chạy cả lưới 15 tổ hợp (Task 6) trên ml-25m thì
        # tích tụ hàng chục GB checkpoint mồ côi trong CHECKPOINT_DIR.
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    )
    builder = _apply_hdfs_config(builder)
    spark = builder.getOrCreate()
    config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(config.CHECKPOINT_DIR))
    return spark
```

- [ ] **Step 4: Chạy test để xác nhận THÀNH CÔNG**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh tests/test_session_hdfs.py tests/test_smoke.py -v
```

Kỳ vọng: toàn bộ PASS.

- [ ] **Step 5: Commit**

```bash
git add src/session.py tests/test_session_hdfs.py
git commit -m "feat: đặt fs.defaultFS khi HDFS_NAMENODE được đặt

Tách thành _apply_hdfs_config() để test được bằng builder giả: getOrCreate()
trả về session đang tồn tại nên test qua get_spark() sẽ tautological.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 3: Thay `pathlib` bằng Hadoop FileSystem trong ingest

**Files:**
- Modify: `src/jobs/ingest.py:28-46`, `src/jobs/ingest.py:85`, `src/jobs/ingest.py:100`
- Test: `tests/test_ingest.py` (thêm test mới)

**Interfaces:**
- Consumes: `config.LAKE_ROOT`, `config.is_remote_lake()` (Task 1)
- Produces: `ingest._path_bytes(spark, path) -> int` — dung lượng của file hoặc thư mục, qua Hadoop FileSystem API, chạy đúng trên cả local lẫn HDFS

**Ghi chú thiết kế:** giữ nguyên `_dir_bytes` cho CSV đầu vào (luôn ở local, và số liệu `csv_bytes` đã commit — không đụng vào để không có rủi ro lệch). Chỉ đầu ra Parquet dùng `_path_bytes`.

- [ ] **Step 1: Viết test thất bại**

Thêm vào cuối `tests/test_ingest.py`:

```python
def test_path_bytes_counts_parquet_output(spark, tmp_path):
    """_path_bytes phải đọc được dung lượng qua Hadoop FS, không qua pathlib.

    Dùng đường dẫn local trong test vì suite không có cụm HDFS; Hadoop FS xử lý
    local qua scheme file:// nên cùng một đoạn code chạy đúng ở cả hai nơi.
    """
    out = tmp_path / "t.parquet"
    spark.range(1000).write.mode("overwrite").parquet(str(out))

    size = ingest._path_bytes(spark, str(out))

    assert size > 0
    # Xấp xỉ tổng dung lượng file thật. Không khẳng định bằng tuyệt đối vì
    # Spark ghi kèm _SUCCESS và các file .crc mà hai cách đếm có thể tính khác nhau.
    actual = ingest._dir_bytes(out)
    assert 0.5 * actual <= size <= 1.5 * actual


def test_path_bytes_handles_single_file(spark, tmp_path):
    """Phải đúng cả khi đường dẫn là MỘT file, không phải thư mục.

    _dir_bytes cũ có nhánh riêng cho file; _path_bytes phải giữ hành vi đó,
    nếu không ingest sẽ báo 0 byte cho đầu vào là file đơn.
    """
    f = tmp_path / "x.txt"
    f.write_text("0123456789", encoding="utf-8")

    assert ingest._path_bytes(spark, str(f)) == 10
```

Nếu đầu file `tests/test_ingest.py` chưa import `ingest`, thêm:

```python
from src.jobs import ingest
```

- [ ] **Step 2: Chạy test để xác nhận THẤT BẠI**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh tests/test_ingest.py -v
```

Kỳ vọng: hai test mới FAIL với `AttributeError: module 'src.jobs.ingest' has no attribute '_path_bytes'`. Các test cũ trong file phải vẫn PASS — Task 1 Step 7 đã sửa `LAKE_DIR` thành `LAKE_ROOT`.

- [ ] **Step 3: Thêm `_path_bytes` vào `src/jobs/ingest.py`**

Chèn ngay sau `_dir_bytes` (dòng 31):

```python
def _path_bytes(spark, path) -> int:
    """Dung lượng của một file hoặc thư mục, qua Hadoop FileSystem API.

    Khác _dir_bytes (dùng pathlib, chỉ chạy được trên hệ thống file cục bộ),
    hàm này đi qua py4j gateway để gọi thẳng Hadoop FileSystem nên hoạt động
    trên MỌI scheme Spark hỗ trợ: file://, hdfs://, s3a://...

    Cần nó vì khi LAKE_ROOT trỏ sang HDFS, config.RATINGS_PARQUET là một chuỗi
    "hdfs://..." và pathlib sẽ hiểu sai hoàn toàn (Path coi nó là đường dẫn
    tương đối tên "hdfs:" rồi báo không tồn tại, dung lượng ra 0).

    getContentSummary().getLength() cộng dồn mọi file trong cây thư mục, nên
    một lời gọi là đủ cho cả file đơn lẫn thư mục Parquet nhiều phần.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(str(path))
    fs = jpath.getFileSystem(hadoop_conf)
    return int(fs.getContentSummary(jpath).getLength())
```

- [ ] **Step 4: Thêm nhánh HDFS vào `mkdir`, và đổi cách đo dung lượng**

Task 1 Step 7 đã đổi dòng 85 thành `Path(config.LAKE_ROOT).mkdir(parents=True, exist_ok=True)`. Giờ bọc nó trong điều kiện:

```python
    # Chỉ tạo thư mục khi lake nằm trên hệ thống file cục bộ. Trên HDFS, Spark
    # tự tạo thư mục khi ghi, và pathlib không hiểu "hdfs://" — gọi Path.mkdir
    # trên chuỗi đó sẽ tạo một thư mục rác tên "hdfs:" ngay trong thư mục làm việc.
    if not config.is_remote_lake():
        Path(config.LAKE_ROOT).mkdir(parents=True, exist_ok=True)
```

Sửa dòng `"parquet_bytes"` trong dict trả về:

```python
        "parquet_bytes": _path_bytes(spark, config.RATINGS_PARQUET),
```

`Path` đã được import sẵn ở `src/jobs/ingest.py:4`, không cần thêm import.

- [ ] **Step 5: Chạy test để xác nhận THÀNH CÔNG**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh tests/test_ingest.py -v
```

Kỳ vọng: toàn bộ PASS.

- [ ] **Step 6: Chạy TOÀN BỘ suite**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh -v
```

Kỳ vọng: **toàn bộ PASS** — 91 test (81 ban đầu + 5 ở Task 1 + 3 ở Task 2 + 2 ở task này). Nếu còn chỗ nào tham chiếu `config.LAKE_DIR`, nó sẽ fail ở đây — sửa nốt rồi chạy lại.

- [ ] **Step 7: Nghiệm thu pipeline bind mount vẫn chạy y như cũ**

Đây là bước **bắt buộc**: xác nhận chưa đặt biến môi trường thì không có gì thay đổi.

```bash
cd docker && docker compose up -d && cd ..
./scripts/run_pipeline.sh ml-latest-small --force
```

Kỳ vọng: exit 0, sinh đủ `ingest_stats.csv`, `best_params.csv`, `metrics.csv`, `recs.sqlite`.

> ⚠️ `--force` ở đây sẽ ghi đè `report/results/*.csv` bằng số liệu `ml-latest-small`. **Sao lưu trước:**
> ```bash
> cp -r report/results report/results_ml25m_backup
> ```
> và khôi phục sau khi nghiệm thu xong:
> ```bash
> rm -rf report/results && mv report/results_ml25m_backup report/results
> ```

- [ ] **Step 8: Commit**

```bash
git add src/jobs/ingest.py tests/test_ingest.py
git commit -m "feat: đo dung lượng lake qua Hadoop FS thay vì pathlib

pathlib không hiểu 'hdfs://' — Path coi nó là đường dẫn tương đối và báo 0 byte.
getContentSummary() chạy đúng trên mọi scheme Spark hỗ trợ. mkdir chỉ gọi khi
lake ở local.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 4: Dựng cụm HDFS bằng Docker Compose

**Files:**
- Create: `docker/hadoop/core-site.xml`
- Create: `docker/hadoop/hdfs-site.xml`
- Modify: `docker/docker-compose.yml`

**Interfaces:**
- Consumes: không có
- Produces:
  - service `namenode` — NameNode UI ở `localhost:9870`, RPC ở `namenode:9000`
  - service `datanode` — 2 replica
  - `SPARK_WORKER_MEMORY` mặc định đổi `3g` → `2g`

**Ghi chú:** task này không có unit test (không test được YAML/XML bằng pytest). Nghiệm thu bằng lệnh kiểm tra thật ở Step 4–6.

- [ ] **Step 1: Tạo `docker/hadoop/core-site.xml`**

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <!-- Hệ thống file mặc định của cụm Hadoop. "namenode" là TÊN SERVICE trong
       docker-compose, Docker DNS nội bộ phân giải thành IP container — không
       hardcode IP ở bất kỳ đâu. -->
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:9000</value>
  </property>
</configuration>
```

- [ ] **Step 2: Tạo `docker/hadoop/hdfs-site.xml`**

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <!-- 2, KHÔNG phải 3: cụm chỉ có 2 DataNode (ngân sách RAM, xem plan).
       Replication lớn hơn số DataNode làm NameNode báo under-replicated vĩnh
       viễn và fsck luôn hiện trạng thái không khoẻ. -->
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>

  <!-- 128 MB, khớp với TARGET_FILE_MB trong src/jobs/ingest.py:12. Giữ hai con
       số bằng nhau để mỗi file Parquet rơi đúng vào một block, không bị cắt đôi. -->
  <property>
    <name>dfs.blocksize</name>
    <value>134217728</value>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/hadoop/dfs/data</value>
  </property>

  <!-- BẮT BUỘC. Container Spark chạy uid 185 (user "spark", xem
       docker/Dockerfile.spark:24), container Hadoop chạy user "hadoop" — uid
       khác nhau. Không tắt kiểm tra quyền, Spark ghi vào HDFS sẽ bị
       AccessControlException. Chấp nhận được trong môi trường nghiên cứu một
       máy; cụm thật phải dùng Kerberos. -->
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
```

- [ ] **Step 3: Thêm service vào `docker/docker-compose.yml`**

Thêm hai service, và sửa mặc định RAM của worker:

```yaml
  namenode:
    image: apache/hadoop:3.3.6
    container_name: namenode
    command: hdfs namenode
    environment:
      HADOOP_HOME: /opt/hadoop
      # Ghim heap 512m: mặc định 1g thừa cho vài nghìn block, và ngân sách RAM
      # của WSL2 (12 GB) đã bị Spark chiếm ~10 GB.
      HDFS_NAMENODE_OPTS: "-Xmx512m"
      # Tự format thư mục metadata ở lần khởi động đầu tiên.
      ENSURE_NAMENODE_DIR: /hadoop/dfs/name
    ports:
      - "9870:9870"   # NameNode Web UI — nơi chụp ảnh cho báo cáo
      - "9000:9000"   # RPC, Spark kết nối qua cổng này
    volumes:
      - ./hadoop/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro
      - ./hadoop/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro
      - namenode-data:/hadoop/dfs/name

  datanode:
    image: apache/hadoop:3.3.6
    command: hdfs datanode
    depends_on:
      - namenode
    environment:
      HADOOP_HOME: /opt/hadoop
      HDFS_DATANODE_OPTS: "-Xmx512m"
    volumes:
      - ./hadoop/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro
      - ./hadoop/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro
    deploy:
      replicas: 2

volumes:
  namenode-data:
```

Và trong service `spark-worker`, đổi mặc định bộ nhớ:

```yaml
      SPARK_WORKER_MEMORY: "${SPARK_WORKER_MEMORY:-2g}"
```

> Dòng này đổi mặc định cho **cả** pipeline bind mount. Đó là chủ đích: chạy đồng thời Spark + 3 JVM Hadoop trên 12 GB bắt buộc phải hạ. Ghi rõ trong commit message để sau này không ai thắc mắc vì sao `ml-25m` chậm hơn số đã đo.

- [ ] **Step 4: Kiểm tra cú pháp compose**

```bash
docker compose -f docker/docker-compose.yml config > /dev/null && echo "YAML HOP LE"
```

Kỳ vọng: in ra `YAML HOP LE`.

- [ ] **Step 5: Dựng cụm và kiểm tra NameNode sống**

```bash
cd docker && docker compose up -d && cd ..
docker compose -f docker/docker-compose.yml ps
```

Kỳ vọng: 6 container ở trạng thái `Up` — `spark-master`, 2 × `spark-worker`, `movielens-api`, `namenode`, 2 × `datanode`.

> **Nếu `namenode` restart liên tục:** `docker logs namenode`. Chi tiết `ENSURE_NAMENODE_DIR` và cách image nạp config có thể khác giữa các bản `apache/hadoop`. Format thủ công rồi khởi động lại:
> ```bash
> docker compose -f docker/docker-compose.yml run --rm namenode hdfs namenode -format -force
> docker compose -f docker/docker-compose.yml up -d namenode
> ```

- [ ] **Step 6: Xác nhận 2 DataNode đã đăng ký**

```bash
MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml exec -T namenode \
  hdfs dfsadmin -report | head -20
```

Kỳ vọng: thấy dòng `Live datanodes (2):`. Nếu là `(0)`, đợi thêm 30 giây (NameNode cần thoát safe mode) rồi chạy lại.

- [ ] **Step 7: Kiểm tra RAM thực tế**

```bash
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}"
```

Kỳ vọng: tổng dưới ~10 GB. Nếu vượt, hạ `SPARK_WORKER_MEMORY` xuống `1g` hoặc giảm còn 1 DataNode (và `dfs.replication=1`).

- [ ] **Step 8: Commit**

```bash
git add docker/hadoop/ docker/docker-compose.yml
git commit -m "feat: thêm cụm HDFS 1 NameNode + 2 DataNode vào compose

replication=2 vì chỉ có 2 DataNode; blocksize 128MB khớp TARGET_FILE_MB;
permissions tắt vì container Spark (uid 185) và Hadoop (user hadoop) khác uid.
Heap Hadoop ghim 512m và SPARK_WORKER_MEMORY hạ 3g->2g để vừa ngân sách
12 GB của WSL2.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 5: Script chạy pipeline với lake trên HDFS

**Files:**
- Create: `scripts/run_pipeline_hdfs.sh`

**Interfaces:**
- Consumes: `config.LAKE_ROOT`, `config.is_remote_lake()` (Task 1); `session._apply_hdfs_config()` (Task 2); `ingest._path_bytes()` (Task 3); service `namenode` (Task 4)
- Produces: `/movielens/lake/{ratings,movies}.parquet` trên HDFS

- [ ] **Step 1: Tạo `scripts/run_pipeline_hdfs.sh`**

```bash
#!/usr/bin/env bash
# Chạy pipeline với tầng lake đặt trên HDFS thay vì bind mount.
#
# Chỉ LAKE đi lên HDFS. raw/, output/ (recs.sqlite, model/) và checkpoint/ vẫn
# ở local — SQLite cần random access và file locking mà HDFS không có, còn
# src/common/eligibility.py dùng shutil.copytree (Python thuần, không hiểu
# "hdfs://"). Xem docs/superpowers/plans/2026-10-01-hdfs-lake-storage.md.
#
# Dùng: ./scripts/run_pipeline_hdfs.sh [ml-latest-small|ml-25m]
set -euo pipefail

# Tắt dịch đường dẫn của MSYS (Git Bash trên Windows) — xem chú thích cùng nội
# dung trong run_pipeline.sh. Không có dòng này, đường dẫn trong container
# "/opt/spark/bin/spark-submit" bị dịch thành đường dẫn Windows và job chết với
# exit 127.
export MSYS_NO_PATHCONV=1

DATASET="${1:-ml-latest-small}"
COMPOSE="docker compose -f $(dirname "$0")/../docker/docker-compose.yml"

NAMENODE="hdfs://namenode:9000"
HDFS_LAKE="$NAMENODE/movielens/lake"

echo "Kiểm tra cụm HDFS..."
if ! $COMPOSE exec -T namenode hdfs dfsadmin -report | grep -q "Live datanodes"; then
  echo "LỖI: không liên lạc được với NameNode. Chạy 'docker compose up -d' trước." >&2
  exit 1
fi
LIVE=$($COMPOSE exec -T namenode hdfs dfsadmin -report \
       | grep "Live datanodes" | grep -o '[0-9]\+' | head -1)
echo "  DataNode đang sống: $LIVE"
if [ "$LIVE" -lt 2 ]; then
  echo "LỖI: cần ít nhất 2 DataNode cho dfs.replication=2, hiện chỉ có $LIVE." >&2
  echo "Đợi thêm 30 giây rồi chạy lại, hoặc kiểm tra 'docker logs namenode'." >&2
  exit 1
fi

echo "Tạo thư mục trên HDFS..."
$COMPOSE exec -T namenode hdfs dfs -mkdir -p /movielens/lake

for job in ingest train_als evaluate export_recs; do
  echo ""
  echo "=============================================="
  echo "  Đang chạy: $job  (dataset=$DATASET, lake=HDFS)"
  echo "=============================================="
  $COMPOSE exec -T spark-master \
    env DATASET="$DATASET" \
        SPARK_MASTER_URL=spark://spark-master:7077 \
        HDFS_NAMENODE="$NAMENODE" \
        LAKE_ROOT="$HDFS_LAKE" \
    /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    "/opt/app/src/jobs/${job}.py"
done

echo ""
echo "Pipeline xong. Kiểm tra block mapping trên HDFS:"
echo "  $COMPOSE exec namenode hdfs fsck /movielens/lake/ratings.parquet -files -blocks -locations"
echo "NameNode UI: http://localhost:9870"
```

- [ ] **Step 2: Cấp quyền chạy**

```bash
chmod +x scripts/run_pipeline_hdfs.sh
```

- [ ] **Step 3: Sao lưu kết quả ml-25m trước khi chạy**

```bash
cp -r report/results report/results_ml25m_backup
ls report/results_ml25m_backup/
```

Kỳ vọng: thấy 5 file CSV. **Không bỏ qua bước này** — pipeline sẽ ghi đè `report/results/*.csv` bằng số liệu `ml-latest-small`.

- [ ] **Step 4: Chạy pipeline trên HDFS**

```bash
./scripts/run_pipeline_hdfs.sh ml-latest-small
```

Kỳ vọng: exit 0, bốn job đều chạy xong.

> **Nếu `ingest` lỗi `AccessControlException`:** `dfs.permissions.enabled` chưa có hiệu lực. Kiểm tra file config đã mount đúng:
> ```bash
> docker compose -f docker/docker-compose.yml exec namenode cat /opt/hadoop/etc/hadoop/hdfs-site.xml
> ```
>
> **Nếu lỗi `UnknownHostException: namenode`:** container Spark và container Hadoop không cùng mạng. Kiểm tra `docker network ls` và `docker inspect`.

- [ ] **Step 5: Xác minh dữ liệu thật sự nằm trên HDFS**

```bash
MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml exec -T namenode \
  hdfs dfs -ls -R /movielens/lake
```

Kỳ vọng: thấy các file `part-*.snappy.parquet` và `_SUCCESS`.

- [ ] **Step 6: Chụp bằng chứng block mapping — đây là ảnh cho báo cáo**

```bash
MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml exec -T namenode \
  hdfs fsck /movielens/lake/ratings.parquet -files -blocks -locations
```

Kỳ vọng: với mỗi file, một dòng dạng

```
... replicated: replication=2, 1 block(s):
0. BP-...:blk_1073741825_1001 len=... Live_repl=2
   [DatanodeInfoWithStorage[172.x.x.x:9866,...], DatanodeInfoWithStorage[172.x.x.x:9866,...]]
```

Lưu output này vào `report/figures/hdfs_fsck.txt`. **Hai địa chỉ DataNode khác nhau trong ngoặc vuông chính là bằng chứng replication đang hoạt động.**

- [ ] **Step 7: Chụp NameNode UI**

Mở http://localhost:9870 và chụp ba tab, lưu vào `report/figures/`:

| Tab | Tên file | Nội dung |
|---|---|---|
| Overview | `hdfs_overview.png` | Dung lượng, số block, số DataNode sống |
| Datanodes | `hdfs_datanodes.png` | Danh sách 2 DataNode, dung lượng dùng mỗi node |
| Utilities → Browse the file system | `hdfs_browse.png` | Cây `/movielens/lake/` với các file Parquet |

- [ ] **Step 8: Khôi phục kết quả ml-25m**

```bash
rm -rf report/results && mv report/results_ml25m_backup report/results
head -2 report/results/ingest_stats.csv
```

Kỳ vọng: dòng thứ hai bắt đầu bằng `ml-25m,25000095,...`. **Nếu thấy `ml-latest-small`, việc khôi phục đã thất bại** — lấy lại từ git: `git checkout report/results/`.

- [ ] **Step 9: Commit**

```bash
git add scripts/run_pipeline_hdfs.sh report/figures/
git commit -m "feat: script chạy pipeline với lake trên HDFS

Kiểm tra số DataNode sống trước khi chạy để không fail giữa chừng với lỗi
under-replicated khó hiểu. Kèm bằng chứng fsck và ảnh NameNode UI.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 6: Đo so sánh và viết tài liệu

**Files:**
- Create: `report/results/storage_comparison.csv`
- Modify: `README.md`
- Modify: `report/outline.md`

**Interfaces:**
- Consumes: `scripts/run_pipeline_hdfs.sh` (Task 5), `scripts/run_pipeline.sh` (có sẵn)
- Produces: bảng số liệu so sánh hai hệ lưu trữ

- [ ] **Step 1: Đo bind mount, 3 lần**

```bash
cp -r report/results report/results_ml25m_backup

for i in 1 2 3; do
  START=$(date +%s.%N)
  ./scripts/run_pipeline.sh ml-latest-small --force > /dev/null 2>&1
  END=$(date +%s.%N)
  awk -v a="$START" -v b="$END" 'BEGIN { printf "bind_mount,%d,%.3f\n", '"$i"', b - a }'
done
```

Ghi lại ba con số. Dùng `awk` chứ không dùng `bc`: `bc` không có trên Git Bash (MSYS) và với `set -e` script sẽ chết ngay sau phép đo đầu tiên.

- [ ] **Step 2: Đo HDFS, 3 lần**

```bash
for i in 1 2 3; do
  START=$(date +%s.%N)
  ./scripts/run_pipeline_hdfs.sh ml-latest-small > /dev/null 2>&1
  END=$(date +%s.%N)
  awk -v a="$START" -v b="$END" 'BEGIN { printf "hdfs,%d,%.3f\n", '"$i"', b - a }'
done

rm -rf report/results && mv report/results_ml25m_backup report/results
```

- [ ] **Step 3: Ghi `report/results/storage_comparison.csv`**

Điền số thật đo được ở Step 1–2 vào:

```csv
storage,dataset,run,total_seconds
bind_mount,ml-latest-small,1,<số đo được>
bind_mount,ml-latest-small,2,<số đo được>
bind_mount,ml-latest-small,3,<số đo được>
hdfs,ml-latest-small,1,<số đo được>
hdfs,ml-latest-small,2,<số đo được>
hdfs,ml-latest-small,3,<số đo được>
```

- [ ] **Step 4: Lấy dung lượng chiếm đĩa trên HDFS**

```bash
MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml exec -T namenode \
  hdfs dfs -du -s -h /movielens/lake
```

Kỳ vọng: hai cột — dung lượng logic và dung lượng thật sau nhân bản (**gấp đôi**, vì `replication=2`). Ghi lại cả hai.

- [ ] **Step 5: Thêm mục "Chạy với HDFS" vào `README.md`**

Chèn sau mục "Chạy pipeline":

````markdown
### Chạy với tầng lake trên HDFS

```bash
cd docker && docker compose up -d && cd ..     # dựng cả Spark lẫn HDFS
./scripts/run_pipeline_hdfs.sh ml-latest-small
```

Chỉ **tầng lake** (`ratings.parquet`, `movies.parquet`) nằm trên HDFS. Tầng raw,
`data/output/` (model, `recs.sqlite`, `item_factors.npy`) và `data/checkpoint/`
vẫn ở local — SQLite cần random access và file locking mà HDFS (write-once-read-many)
không hỗ trợ, còn `src/common/eligibility.py` dùng `shutil.copytree` của Python
thuần, không hiểu `hdfs://`. Đây là lựa chọn kiến trúc có chủ đích: mỗi tầng
dùng hệ lưu trữ phù hợp với kiểu truy cập của nó.

Không đặt `HDFS_NAMENODE`/`LAKE_ROOT` thì pipeline chạy y hệt như trước trên
bind mount — `./scripts/run_pipeline.sh` không bị ảnh hưởng.

**URL:**
- NameNode Web UI: http://localhost:9870 (Overview, Datanodes, Browse the file system)

**Kiểm tra block mapping và replica placement:**

```bash
docker compose -f docker/docker-compose.yml exec namenode \
  hdfs fsck /movielens/lake/ratings.parquet -files -blocks -locations
```

> **Ngân sách RAM.** Chạy đồng thời Spark và HDFS cần WSL2 ≥ 12 GB. Heap Hadoop
> đã ghim 512 MB và `SPARK_WORKER_MEMORY` mặc định hạ từ 3g xuống 2g. **Không
> chạy `ml-25m` khi cụm HDFS đang bật** — nguy cơ WSL2 OOM-kill worker giữa chừng,
> triệu chứng là container biến mất im lặng hoặc "Connection reset", không có
> exception Spark nào.
````

- [ ] **Step 6: Thêm mục phân tích vào `report/outline.md`**

Thêm một tiểu mục vào phần kiến trúc:

```markdown
### So sánh hai hệ lưu trữ: Docker bind mount vs HDFS

| Hạng mục | Bind mount | HDFS (replication=2) |
|---|---|---|
| Thời gian pipeline (ml-latest-small, trung vị 3 lần) | ⚠️ điền từ storage_comparison.csv | ⚠️ điền |
| Dung lượng đĩa chiếm | ⚠️ điền | ⚠️ điền (gấp đôi do nhân bản) |
| Chịu lỗi | không | có **về cơ chế**, không về vật lý — mọi DataNode chung một ổ SSD |
| Data locality | không áp dụng | có **cơ chế**, không có **lợi ích** — mọi node đọc cùng một đĩa |
| Mở rộng dung lượng | giới hạn bởi ổ host | giới hạn bởi ổ host (chỉ một máy) |

**Nhận định.** Triển khai HDFS trên một máy đơn làm tăng thời gian xử lý và gấp
đôi dung lượng lưu trữ mà **không** đem lại khả năng chịu lỗi thực sự: cả hai
replica nằm trên cùng một ổ đĩa vật lý, ổ hỏng là mất cả hai. Tương tự, data
locality không có ý nghĩa khi mọi DataNode đọc chung một thiết bị.

Điều này cho thấy HDFS không phải lựa chọn mặc định cho mọi bài toán "big data".
Lợi ích của nó — chịu lỗi, data locality, mở rộng dung lượng tuyến tính — chỉ
hiện thực hoá khi cụm trải trên nhiều máy vật lý. Giá trị của việc triển khai ở
quy mô này là **xác nhận tính tương thích của mã nguồn** (đổi `LAKE_ROOT` sang
cụm thật là chạy ngay, không sửa code) và **đo được chi phí của lớp trừu tượng**.

Kết luận này cùng hướng với kết quả đo mở rộng cụm Spark ở mục trước, nơi tăng
số worker từ 2 lên 4 làm pipeline **chậm đi** (178s → 227s): cả hai đều cho thấy
hạ tầng phân tán có chi phí cố định, và chi phí đó chỉ được bù lại khi quy mô dữ
liệu vượt quá năng lực một máy.
```

- [ ] **Step 7: Chạy toàn bộ test lần cuối**

```bash
MSYS_NO_PATHCONV=1 ./scripts/test.sh -v
```

Kỳ vọng: toàn bộ PASS.

- [ ] **Step 8: Xác nhận số liệu ml-25m còn nguyên**

```bash
head -2 report/results/ingest_stats.csv
git status --short report/results/
```

Kỳ vọng: dòng thứ hai bắt đầu bằng `ml-25m,25000095,678260987,...`, và `git status` chỉ hiện `storage_comparison.csv` là file mới.

- [ ] **Step 9: Commit**

```bash
git add report/results/storage_comparison.csv README.md report/outline.md
git commit -m "docs: so sánh bind mount vs HDFS và hướng dẫn chạy

HDFS trên một máy đơn chậm hơn và gấp đôi dung lượng mà không có chịu lỗi thật
(mọi replica chung một ổ). Giá trị là xác nhận mã nguồn tương thích và đo được
chi phí của lớp trừu tượng.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Bảng Rủi Ro

| Rủi ro | Dấu hiệu | Xử lý |
|---|---|---|
| **WSL2 OOM-kill** | container biến mất im lặng; "Connection reset"; không có exception Spark | Hạ `SPARK_WORKER_MEMORY` xuống `1g`; hoặc còn 1 DataNode + `dfs.replication=1`. **Không bao giờ chạy `ml-25m` khi HDFS đang bật** |
| **Ghi đè số liệu ml-25m** | `report/results/ingest_stats.csv` dòng 2 bắt đầu bằng `ml-latest-small` | `git checkout report/results/` — chúng đã được commit |
| **`AccessControlException` khi Spark ghi HDFS** | job `ingest` chết ngay bước ghi Parquet | `dfs.permissions.enabled=false` chưa có hiệu lực; kiểm tra file đã mount đúng vào `/opt/hadoop/etc/hadoop/` |
| **NameNode restart liên tục** | `docker compose ps` hiện `Restarting` | `docker logs namenode`; format thủ công bằng `hdfs namenode -format -force` |
| **`UnknownHostException: namenode`** | Spark không phân giải được tên | Container Spark và Hadoop không cùng mạng compose; kiểm tra `docker network ls` |
| **Thư mục rác tên `hdfs:` xuất hiện** | có thư mục `hdfs:` trong thư mục làm việc | Còn sót một chỗ gọi `Path(...)` trên chuỗi URI; grep `Path(` quanh `LAKE_ROOT` |
| **`fsck` báo under-replicated mãi** | `Under-replicated blocks: N` | `dfs.replication` lớn hơn số DataNode sống; đặt bằng đúng số DataNode |

## Cách Hoàn Tác Toàn Bộ

```bash
git checkout main
cd docker && docker compose down && docker compose up -d && cd ..
```

Branch `feat/hdfs` giữ nguyên để tham khảo. Vì mặc định không đặt `HDFS_NAMENODE`/`LAKE_ROOT`, ngay cả khi đã merge thì pipeline cũ vẫn chạy như trước — **chỉ trừ** `SPARK_WORKER_MEMORY` đã hạ 3g → 2g ở Task 4, muốn khôi phục thì sửa lại dòng đó.

## Ước Lượng Thời Gian

| Task | Thời gian | Ghi chú |
|---|---|---|
| 1. `LAKE_ROOT` trong config | 30 phút | |
| 2. `fs.defaultFS` trong session | 30 phút | |
| 3. Hadoop FS trong ingest | 45 phút | |
| 4. Compose + config Hadoop | 1–2 giờ | phần lớn là debug image lần đầu |
| 5. Script + nghiệm thu + chụp ảnh | 1 giờ | |
| 6. Đo so sánh + tài liệu | 1 giờ | 6 lần chạy `ml-latest-small` |
| **Tổng** | **4,5–6 giờ** | |
