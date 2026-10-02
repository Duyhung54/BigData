"""Tập trung mọi đường dẫn và hằng số cấu hình.

Không hardcode đường dẫn hay ngưỡng ở bất kỳ file nào khác.
"""
import os
from pathlib import Path

# ---------- Cấu Hình Storage (Local vs HDFS) ----------
LOCAL_DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/data"))

# Nếu USE_HDFS=true, Spark sẽ lưu file lớn (Parquet, Delta, Model) lên HDFS
USE_HDFS = os.environ.get("USE_HDFS", "false").lower() == "true"
HDFS_PREFIX = "hdfs://namenode:9000" if USE_HDFS else ""

# Đường dẫn dùng cho Spark (Chuỗi string để ghép với HDFS)
DATA_ROOT_STR = HDFS_PREFIX + "/opt/data"
LAKE_DIR_STR = DATA_ROOT_STR + "/lake"
OUTPUT_DIR_STR = DATA_ROOT_STR + "/output"
CHECKPOINT_DIR_STR = DATA_ROOT_STR + "/checkpoint"

# Đường dẫn dùng cho Local/FastAPI/SQLite (Kiểu Path)
DATA_ROOT = LOCAL_DATA_ROOT
RAW_DIR = LOCAL_DATA_ROOT / "raw"
LAKE_DIR = LOCAL_DATA_ROOT / "lake"
OUTPUT_DIR = LOCAL_DATA_ROOT / "output"
CHECKPOINT_DIR = LOCAL_DATA_ROOT / "checkpoint"
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/opt/app/report/results"))

# ---------- Streaming ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_RATINGS_TOPIC = os.getenv("KAFKA_RATINGS_TOPIC", "ratings")

# Delta lake (giao dịch)
DELTA_LAKE_PATH = LAKE_DIR_STR + "/ratings_delta"
DATASET = os.environ.get("DATASET", "ml-25m")

RATINGS_CSV = RAW_DIR / DATASET / "ratings.csv"
MOVIES_CSV = RAW_DIR / DATASET / "movies.csv"

RATINGS_PARQUET = LAKE_DIR_STR + "/ratings.parquet"
MOVIES_PARQUET = LAKE_DIR_STR + "/movies.parquet"

MODEL_DIR = OUTPUT_DIR_STR + "/model"
RECS_PARQUET = OUTPUT_DIR_STR + "/recommendations.parquet"
ITEM_INDEX_PARQUET = OUTPUT_DIR_STR + "/item_index.parquet"

# ---------- Local Outputs (SQLite & numpy) ----------
SCALING_RESULTS_DIR = RESULTS_DIR / "scaling_scratch"
SCALING_MODEL_DIR = OUTPUT_DIR_STR + "/model_scaling_scratch"
RECS_SQLITE = OUTPUT_DIR / "recs.sqlite"
ITEM_FACTORS_NPY = OUTPUT_DIR / "item_factors.npy"

# Chia tập theo thời gian trong từng user
SPLIT_TRAIN = 0.70
SPLIT_VAL = 0.85
MIN_RATINGS_PER_USER = 5

# Đánh giá
RELEVANCE_THRESHOLD = 4.0
TOP_K = 10
N_RECOMMENDATIONS = 20

MIN_RATINGS_FOR_RECOMMENDATION = 20

# Lưới siêu tham số
ALS_RANKS = [10, 50, 100]
ALS_REG_PARAMS = [0.01, 0.1, 0.2, 0.3, 0.5]
ALS_MAX_ITER = 10
ALS_CHECKPOINT_INTERVAL = 5

ALS_FIXED_RANK = int(os.environ["ALS_RANK"]) if os.environ.get("ALS_RANK") else None
ALS_FIXED_REG_PARAM = float(os.environ["ALS_REG_PARAM"]) if os.environ.get("ALS_REG_PARAM") else None

def is_scaling_run() -> bool:
    return ALS_FIXED_RANK is not None and ALS_FIXED_REG_PARAM is not None
