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

# Lưới siêu tham số
ALS_RANKS = [10, 50, 100]
ALS_REG_PARAMS = [0.01, 0.1, 0.2]
ALS_MAX_ITER = 10
ALS_CHECKPOINT_INTERVAL = 5

# Khi cả hai được đặt, train_als.py bỏ qua grid search và chỉ fit một tổ hợp.
# Thí nghiệm đo scale ở Task 11 cần điều này để phép đo chỉ gồm một lần fit.
ALS_FIXED_RANK = int(os.environ["ALS_RANK"]) if os.environ.get("ALS_RANK") else None
ALS_FIXED_REG_PARAM = float(os.environ["ALS_REG_PARAM"]) if os.environ.get("ALS_REG_PARAM") else None
