"""FastAPI phục vụ gợi ý. Không import pyspark.

Store được nạp LƯỜI (lazy): tầng serving phải khởi động được ngay cả khi
tầng batch (Task 1-8) chưa chạy lần nào, vì `docker compose up` khởi động
`app` trước khi ai đó chạy pipeline. Khởi tạo Store ở cấp module sẽ mở
SQLite `mode=ro` ngay lúc import — nếu recs.sqlite chưa tồn tại, import
ném lỗi và container "app" crash-loop mãi mãi, kể cả khi lát nữa dữ liệu
sẽ có. Thay vào đó, mỗi request tự hỏi get_store(): nếu file chưa có thì
trả 503 kèm thông điệp rõ ràng (Task 10 hiển thị thông điệp này); nếu có
thì nạp Store một lần và giữ lại cho các request sau — nên khi pipeline
chạy xong TRONG LÚC server đang sống, request kế tiếp tự phục vụ được mà
không cần khởi động lại.
"""
import os
from pathlib import Path
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import sqlite3
import json
import time

from pydantic import BaseModel
from kafka import KafkaProducer

class RatingEvent(BaseModel):
    userId: int
    movieId: int
    rating: float
    timestamp: Optional[int] = None

class BatchRatingRequest(BaseModel):
    events: List[RatingEvent]

_producer = None

def get_kafka_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return _producer

from serving.store import Store

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/data"))
RECS_SQLITE = Path(os.environ.get("RECS_SQLITE", DATA_ROOT / "output" / "recs.sqlite"))
ITEM_FACTORS_NPY = Path(os.environ.get("ITEM_FACTORS_NPY", DATA_ROOT / "output" / "item_factors.npy"))
ITEM_INDEX_PARQUET = Path(os.environ.get("ITEM_INDEX_PARQUET", DATA_ROOT / "output" / "item_index.parquet"))
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="MovieLens ALS Recommender")

_store: Optional[Store] = None
MODEL_PATH = Path("/opt/app/data/output/model/recs.sqlite")
_last_mtime = 0
# Optional[...] chứ không dùng "X | None": test chạy trong container Spark
# (Python 3.8), nơi cú pháp "|" cho kiểu dữ liệu lỗi ngay lúc import module.
_connection: Optional[sqlite3.Connection] = None
def get_connection() -> sqlite3.Connection:
    global _connection, _last_mtime
    mtime = MODEL_PATH.stat().st_mtime
    if _connection is None or mtime != _last_mtime:
        if _connection:
            _connection.close()
        _connection = sqlite3.connect(str(MODEL_PATH))
        _last_mtime = mtime
        print(f"[API] Reloaded model (mtime={mtime})")
    return _connection
@app.get("/recommendations/{user_id}")
def recommend(user_id: int, k: int = 10):
    con = get_connection()
    cur = con.cursor()
    cur.execute(
        "SELECT movieId, rating FROM recommendations WHERE userId=? ORDER BY rating DESC LIMIT ?",
        (user_id, k)
    )
    rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="User not found or no recommendations yet")
    return [{"movieId": r[0], "score": r[1]} for r in rows]

_last_store_mtime = 0

def get_store() -> Store:
    """Trả về Store đã nạp, nạp lần đầu khi được gọi (không nạp lúc import).
    
    Tự động nạp lại (hot-reload) Store nếu file recs.sqlite có thời gian chỉnh sửa mới 
    hơn lần nạp trước (do job streaming vừa chạy xong).
    """
    global _store, _last_store_mtime
    
    missing = [str(p) for p in (RECS_SQLITE, ITEM_FACTORS_NPY, ITEM_INDEX_PARQUET) if not p.exists()]
    if missing:
        raise HTTPException(
            status_code=503,
            detail=(
                "Dữ liệu gợi ý chưa sẵn sàng, còn thiếu: "
                + ", ".join(missing)
                + ". Hãy chạy pipeline batch trước."
            ),
        )
        
    current_mtime = RECS_SQLITE.stat().st_mtime
    if _store is None or current_mtime != _last_store_mtime:
        _store = Store(RECS_SQLITE, ITEM_FACTORS_NPY, ITEM_INDEX_PARQUET)
        _last_store_mtime = current_mtime
        print(f"[API] Reloaded Store (mtime={current_mtime})")

    return _store


@app.post("/api/rate")
def submit_rating(event: RatingEvent):
    producer = get_kafka_producer()
    rating_dict = event.dict()
    if not rating_dict.get("timestamp"):
        rating_dict["timestamp"] = int(time.time())
    producer.send("ratings", rating_dict)
    producer.flush()
    return {"status": "success", "event": rating_dict}


@app.post("/api/rate/batch")
def submit_rating_batch(req: BatchRatingRequest):
    producer = get_kafka_producer()
    now = int(time.time())
    for ev in req.events:
        d = ev.dict()
        if not d.get("timestamp"):
            d["timestamp"] = now
        producer.send("ratings", d)
    producer.flush()
    return {"status": "success", "sent": len(req.events)}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/users/{user_id}/recommendations")
def recommendations(user_id: int, k: int = Query(10, ge=1, le=20), store: Store = Depends(get_store)):
    items = store.recommendations(user_id, k)
    if not items:
        raise HTTPException(status_code=404, detail=f"Không có gợi ý cho user {user_id}")
    return {"userId": user_id, "items": items}


@app.get("/api/users/{user_id}/history")
def history(user_id: int, k: int = Query(10, ge=1, le=50), store: Store = Depends(get_store)):
    """Trả 200 kèm items rỗng khi user không có rating nào, KHÔNG trả 404.

    Cố ý khác `recommendations`/`similar`, nơi rỗng nghĩa là "không tìm
    thấy đối tượng" nên là lỗi. Ở đây user tồn tại nhưng chưa có/không có
    rating đạt ngưỡng liên quan là một trạng thái hợp lệ, không phải lỗi —
    và giao diện T10 đặt history cạnh recommendations, nơi một cột rỗng
    hiển thị bình thường còn 404 sẽ cần xử lý riêng.
    """
    return {"userId": user_id, "items": store.history(user_id, k)}


@app.get("/api/movies/{movie_id}/similar")
def similar(movie_id: int, k: int = Query(10, ge=1, le=50), store: Store = Depends(get_store)):
    items = store.similar(movie_id, k)
    if not items:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy phim {movie_id}")
    return {"movieId": movie_id, "items": items}


@app.get("/api/movies/search")
def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50), store: Store = Depends(get_store)):
    return {"query": q, "items": store.search(q, limit)}


@app.get("/api/hdfs/stats")
def hdfs_stats():
    import urllib.request
    try:
        url = "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            bean = data["beans"][0]
            live_nodes = json.loads(bean.get("LiveNodes", "{}"))
            nodes_info = []
            for node_addr, info in live_nodes.items():
                nodes_info.append({
                    "node": node_addr.split(":")[0],
                    "used_mb": round(info.get("usedSpace", 0) / (1024 * 1024), 2),
                    "capacity_gb": round(info.get("capacity", 0) / (1024 * 1024 * 1024), 2),
                    "blocks": info.get("numBlocks", 0),
                    "lastContact": info.get("lastContact", 0)
                })
            return {
                "status": "online",
                "total_blocks": bean.get("TotalBlocks", 0),
                "used_mb": round(bean.get("Used", 0) / (1024 * 1024), 2),
                "nodes": nodes_info
            }
    except Exception as e:
        return {"status": "error", "message": str(e), "nodes": []}


@app.get("/api/stats")
def stats(store: Store = Depends(get_store)):
    return store.stats()


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    def index():
        return FileResponse(STATIC_DIR / "index.html")
