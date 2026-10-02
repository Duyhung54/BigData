"""Tập trung mọi đường dẫn và hằng số cấu hình hệ thống Big Data.
Không hardcode đường dẫn hay ngưỡng ở bất kỳ file nào khác.
"""
import os
from pathlib import Path

# ---------- Cấu hình Storage (Local & HDFS) ----------
LOCAL_DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/data"))
USE_HDFS = os.environ.get("USE_HDFS", "true").lower() == "true"
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000")

# Chuẩn HDFS 128 MB và Replication 3 cho 3 DataNodes
HDFS_REPLICATION = 3
HDFS_BLOCKSIZE = 134217728  # 128 MB (128 * 1024 * 1024 bytes)

# Đường dẫn HDFS
HDFS_PATH = f"{HDFS_NAMENODE}/processed/"
HDFS_RAW_PATH = f"{HDFS_NAMENODE}/raw/"

# Đường dẫn Local (Mount Volume)
DATA_ROOT = LOCAL_DATA_ROOT
RAW_DIR = LOCAL_DATA_ROOT / "raw"
OUTPUT_DIR = LOCAL_DATA_ROOT / "output"
CHECKPOINT_DIR = LOCAL_DATA_ROOT / "checkpoint"
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/opt/app/report/results"))

# ---------- Bộ dữ liệu MovieLens ----------
DATASET = os.environ.get("DATASET", "ml-32m")
RAW_DATASET_DIR = RAW_DIR / DATASET

# 6 file CSV thô đầu vào
RATINGS_CSV = RAW_DATASET_DIR / "ratings.csv"
MOVIES_CSV = RAW_DATASET_DIR / "movies.csv"
TAGS_CSV = RAW_DATASET_DIR / "tags.csv"
LINKS_CSV = RAW_DATASET_DIR / "links.csv"
GENOME_SCORES_CSV = RAW_DATASET_DIR / "genome-scores.csv"
GENOME_TAGS_CSV = RAW_DATASET_DIR / "genome-tags.csv"

# 6 file Parquet nén Snappy trên HDFS (hoặc local fallback)
LAKE_PREFIX = HDFS_PATH if USE_HDFS else str(LOCAL_DATA_ROOT / "lake") + "/"
RATINGS_PARQUET = LAKE_PREFIX + "ratings.parquet"
MOVIES_PARQUET = LAKE_PREFIX + "movies.parquet"
TAGS_PARQUET = LAKE_PREFIX + "tags.parquet"
LINKS_PARQUET = LAKE_PREFIX + "links.parquet"
GENOME_SCORES_PARQUET = LAKE_PREFIX + "genome-scores.parquet"
GENOME_TAGS_PARQUET = LAKE_PREFIX + "genome-tags.parquet"

# Delta Lake và Model trên HDFS
DELTA_LAKE_PATH = LAKE_PREFIX + "ratings_delta"
MODEL_DIR = (f"{HDFS_NAMENODE}/models/als_model" if USE_HDFS else str(OUTPUT_DIR / "model"))

# ---------- Message Broker & Streaming (Kafka) ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
KAFKA_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
KAFKA_RATINGS_TOPIC = KAFKA_TOPIC

# ---------- Cơ sở dữ liệu Serving (PostgreSQL & Redis) ----------
POSTGRES_DB = os.getenv("POSTGRES_DB", "movielens_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "adminpassword")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
POSTGRES_JDBC_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ---------- Siêu tham số Huấn luyện ALS ----------
# Chia tập dữ liệu 80% Train, 10% Val, 10% Test theo thời gian
SPLIT_TRAIN = 0.80
SPLIT_VAL = 0.90
MIN_RATINGS_PER_USER = 5
MIN_RATINGS_FOR_RECOMMENDATION = 20
N_RECOMMENDATIONS = 20

# Lưới tìm kiếm siêu tham số
ALS_RANKS = [10]
ALS_REG_PARAMS = [0.05, 0.1]
ALS_MAX_ITER = 10
ALS_CHECKPOINT_INTERVAL = 5

ALS_FIXED_RANK = int(os.environ["ALS_RANK"]) if os.environ.get("ALS_RANK") else None
ALS_FIXED_REG_PARAM = float(os.environ["ALS_REG_PARAM"]) if os.environ.get("ALS_REG_PARAM") else None

def is_scaling_run() -> bool:
    return ALS_FIXED_RANK is not None and ALS_FIXED_REG_PARAM is not None
