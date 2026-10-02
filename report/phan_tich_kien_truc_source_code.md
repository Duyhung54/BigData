# BÁO CÁO PHÂN TÍCH TOÀN DIỆN MÃ NGUỒN VÀ CẤU HÌNH DỰ ÁN
## Đề tài: Hệ Thống Đề Xuất Phim Phân Tán (MovieLens + HDFS + Spark + Kafka + Delta Lake)
### Chức danh thẩm định: Senior Big Data Engineer
### Ngày cập nhật: 02/10/2026 (Theo commit mới nhất: `fd37dc9`)

---

## 1. SƠ ĐỒ KIẾN TRÚC & CÁC SERVICE TRONG `docker-compose.yml`

Hệ thống được định nghĩa trong [`docker/docker-compose.yml`](file:///D:/learning/bigdata/final/docker/docker-compose.yml) bao gồm **10 services** được phân bổ theo 5 tầng chức năng:

| Tầng chức năng | Tên Service | Image / Cổng mở ra Host | Vai trò kiến trúc trong hệ thống |
| :--- | :--- | :--- | :--- |
| **Lưu trữ phân tán (HDFS)** *(Kích hoạt khi bật profile `hdfs`)* | **`namenode`** | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`<br>Ports: `9870` (Web UI), `9000` (IPC) | **HDFS Master**: Quản lý Metadata, Namespace và bảng ánh xạ các khối dữ liệu vật lý. |
| | **`datanode1`** | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | **HDFS Worker 1**: Lưu trữ các khối dữ liệu vật lý (Block size 128 MB). |
| | **`datanode2`** | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | **HDFS Worker 2**: Lưu trữ các bản sao dữ liệu (`dfs.replication = 2`). |
| **Tính toán phân tán (Spark)** | **`spark-master`** | `movielens-spark:latest`<br>Ports: `8080` (Master UI), `7077` (RPC), `4040` (Driver UI) | **Spark Master**: Quản lý tài nguyên cụm, tiếp nhận job và điều phối DAG thực thi. |
| | **`spark-worker`** | `movielens-spark:latest`<br>(2 replicas, 3 Cores + 3 GB RAM/node) | **Spark Workers**: Thực thi các tác vụ tính toán phân tán (Tasks/Executors). |
| **Điều phối thông điệp (Kafka)** | **`zookeeper`** | `confluentinc/cp-zookeeper:7.6.1`<br>Port: `2181` | Quản trị cấu hình, đồng bộ trạng thái và bầu cử cho cụm Kafka. |
| | **`kafka`** | `confluentinc/cp-kafka:7.6.1`<br>Port: `29092` (Host), `9092` (Internal) | **Message Broker phân tán**: Tiếp nhận luồng đánh giá phim theo thời gian thực (Topic: `ratings`). |
| **Xử lý dòng & Tái huấn luyện** | **`stream-ingest`** | `movielens-spark:latest`<br>(Giới hạn 2 Cores) | **Spark Structured Streaming**: Đọc rating liên tục từ Kafka $\rightarrow$ ghi micro-batch vào **Delta Lake**. |
| | **`train-als-stream`** | `movielens-spark:latest` | **Background Worker**: Vòng lặp 60s liên tục cập nhật model ALS khi phát hiện có dữ liệu stream mới trong Delta Lake. |
| **Phục vụ ứng dụng (Serving)** | **`app`** (`movielens-api`) | `movielens-app:latest`<br>Port: `8000` (Web UI & REST API) | **FastAPI Gateway**: Cung cấp API gợi ý phim, Web Dashboard, và nhận rating mới (`POST /api/rate`) gửi vào Kafka. |

### Đánh giá hiện trạng hạ tầng:
* **HDFS**: Đã tích hợp đầy đủ NameNode và 2 DataNode (Replication = 2), có cơ chế chuyển đổi linh hoạt qua biến `USE_HDFS` và Docker Profile `hdfs`.
* **Jupyter Notebook**: **KHÔNG CÓ** trong docker-compose (toàn bộ pipeline chạy dạng script batch/streaming qua `spark-submit`).
* **Cơ sở dữ liệu (DB)**: **KHÔNG DÙNG DATABASE CONTAINER RIÊNG** (như MySQL/PostgreSQL). Hệ thống sử dụng **SQLite file-based (`recs.sqlite`)** có cơ chế **Hot-reload tự động theo thời gian sửa đổi (`mtime`)** khi mô hình stream được huấn luyện lại.

---

## 2. DATA PIPELINE: LUỒNG DỮ LIỆU ĐI TỪ ĐÂU ĐẾN ĐÂU?

Hệ thống vận hành theo **Kiến trúc Lambda / Kappa kết hợp** với 2 nhánh dữ liệu:

```
[ratings.csv thô] ──(Job 1: ingest.py)──▶ [HDFS Data Lake: ratings.parquet (128 MB)] ──(Job 2: train_als.py)──▶ [Model ALS] ──▶ [recs.sqlite]
                                                                                                                   ▲                      │
                                                                                                                   │                      ▼
[Web UI / Simulator] ──▶ [Kafka Topic: "ratings"] ──(stream_ingest.py)──▶ [HDFS Delta Lake: ratings_delta] ───────┴───────────────▶ [FastAPI (Hot-Reload)]
```

| Luồng dữ liệu | Nguồn phát sinh (Source) | Lưu trữ trên HDFS | Công nghệ xử lý | Nơi lưu trữ đích (Target) |
| :--- | :--- | :--- | :--- | :--- |
| **Nhánh Batch (Lịch sử)** | `data/raw/<dataset>/ratings.csv` và `movies.csv` | Lưu tại `hdfs://namenode:9000/opt/data/lake/ratings.parquet` (2 blocks chuẩn 128 MB) | Apache Spark Batch (`ingest.py` $\rightarrow$ `train_als.py` $\rightarrow$ `evaluate.py` $\rightarrow$ `export_recs.py`) | Model lưu tại `/opt/data/output/model/`. Kết quả phục vụ lưu tại `data/output/recs.sqlite` (có B-Tree index). |
| **Nhánh Stream (Thời gian thực)** | Người dùng đánh giá trên Web (`POST /api/rate`) hoặc chạy `simulate_bulk_stream.py` | Lưu tại `hdfs://namenode:9000/opt/data/lake/ratings_delta` (Định dạng ACID Delta Lake) | **Spark Structured Streaming** (`stream_ingest.py`, micro-batch 10s) ghi Delta Lake $\rightarrow$ `train_als_stream.py` đọc gia tăng fit lại model | Cập nhật trực tiếp vào `recs.sqlite`. FastAPI tự động phát hiện `mtime` thay đổi để **Hot-Reload Model ngay tức thì** mà không cần restart server. |

---

## 3. CÁC MODEL VÀ THUẬT TOÁN ĐANG CÓ TRONG DỰ ÁN

* **Thư mục `/notebooks`**: **KHÔNG TỒN TẠI** trong cấu trúc dự án.
* **Các file huấn luyện và thuật toán trong `/src`**:

| File huấn luyện | Thuật toán sử dụng | Vai trò trong hệ thống | File chính |
| :--- | :--- | :--- | :---: |
| [`src/jobs/train_als.py`](file:///D:/learning/bigdata/final/src/jobs/train_als.py) | **Spark MLlib ALS (Alternating Least Squares)** | Huấn luyện Batch trên toàn bộ lịch sử: Chia Temporal Split (70/15/15), chạy **Grid Search 15 tổ hợp** ($rank \times regParam$). | ⭐ **FILE CHÍNH (BATCH)** |
| [`src/jobs/train_als_stream.py`](file:///D:/learning/bigdata/final/src/jobs/train_als_stream.py) | **Spark MLlib ALS** | Huấn luyện lại liên tục (Retraining): Đọc dữ liệu gia tăng từ Delta Lake, gộp với Parquet cũ, cố định tham số ($rank=10, regParam=0.1$) để tránh tràn bộ nhớ đệm `/tmp`. | ⭐ **FILE CHÍNH (STREAM)** |
| [`src/common/baselines.py`](file:///D:/learning/bigdata/final/src/common/baselines.py) | **Global Mean, Item Mean, Popularity** | 3 mô hình cơ sở dùng trong [`src/jobs/evaluate.py`](file:///D:/learning/bigdata/final/src/jobs/evaluate.py) để đối chiếu, chứng minh ALS vượt trội hơn. | File phụ trợ |
| **LightFM / Two-Tower** | **KHÔNG CÓ** | Hoàn toàn chưa được cài đặt trong mã nguồn. | - |
| **Content-Based** | **Một phần ở [`serving/store.py`](file:///D:/learning/bigdata/final/serving/store.py)** | Tìm phim tương tự qua Cosine Similarity trên vector nhân tử ẩn `item_factors.npy`. **Chưa khai thác thẻ Tag Genome**. | - |

---

## 4. BẢNG PHÂN TÍCH THIẾU SÓT (GAP ANALYSIS)

So với mục tiêu hoàn chỉnh của đề tài **"Hệ thống đề xuất phim dùng MovieLens + HDFS + Spark"**:

| Module yêu cầu | Trạng thái hiện tại | Đánh giá kỹ thuật | Điểm còn thiếu cần hoàn thiện |
| :--- | :---: | :--- | :--- |
| **Hạ tầng HDFS** |  Đã có code | Đã thêm NameNode + 2 DataNode vào Docker Compose, có biến `USE_HDFS` chuyển đổi linh hoạt. | Mới có 2 DataNode (Rep=2). Chuẩn bài học thực hành là 3 DataNode (Rep=3). |
| **ETL & Data Lake** |  Đã có code | Đã có Parquet Snappy phân khối 128 MB (Batch) và Delta Lake (Stream). | **Mới dùng 2/6 tệp** (`ratings`, `movies`). Còn bỏ phí `genome-scores.csv` (15.58M dòng), `tags.csv` (1.09M dòng), `links.csv`. |
| **Distributed EDA** | ❌ **CHƯA CÓ** | Thiếu script phân tích phân tán bằng Spark SQL để khám phá dữ liệu thô (đo độ thưa ma trận, phân bố đuôi dài, kiểm tra null). | Cần viết thêm job Spark SQL phân tích (tương tự `step3_sparksql.py` ở bài tập `exe2`). |
| **Train Model** |  **HOÀN THÀNH** | Đã có cả Train Batch với Grid Search lẫn Train Streaming cập nhật liên tục qua Delta Lake. | Có thể mở rộng thêm Content-Based từ Genome cho Cold-Start nếu muốn điểm tối đa. |
| **Evaluate** |  **HOÀN THÀNH** | Đầy đủ chỉ số: RMSE, MAE, NDCG@10, Precision@10, Recall@10, Coverage, 3 Baselines và script vẽ đồ thị trực quan. | Đã hoàn chỉnh. |
| **API Serve & Demo UI**|  **HOÀN THÀNH** | FastAPI RESTful API (< 16ms), có endpoint `POST /api/rate` đẩy vào Kafka, Hot-reload model tự động, Dashboard Web UI hoàn chỉnh. | Đã hoàn chỉnh. |

---

## 5. SO SÁNH BATCH VÀ STREAM TRONG DỰ ÁN

| Tiêu chí | Nhánh BATCH (Đã có trong code) | Nhánh STREAM (Đã có trong code) |
| :--- | :--- | :--- |
| **File thực thi** | `src/jobs/ingest.py`, `train_als.py`, `evaluate.py`, `export_recs.py` | `src/jobs/stream_ingest.py`, `train_als_stream.py`, `serving/api.py` |
| **Cơ chế đọc** | Đọc toàn bộ tệp Parquet/CSV 1 lần theo lô định kỳ (Offline Pipeline). | Đọc liên tục theo dòng sự kiện từ **Kafka topic `ratings`** thông qua `spark.readStream`. |
| **Độ trễ xử lý** | Vài chục giây đến vài phút (quét hàng chục triệu bản ghi). | Micro-batch **10 giây** (Spark Structured Streaming ghi Delta Lake). |
| **Lưu trữ trung gian** | Tệp Parquet nén Snappy (128 MB) trên HDFS Lake Zone. | Bảng **Delta Lake** (`ratings_delta`) hỗ trợ ghi nối tiếp (ACID append). |
| **Đồng bộ về Web** | Xuất kết quả trước vào `recs.sqlite` một lần. | Vòng lặp `train_als_stream.py` tự cập nhật SQLite $\rightarrow$ FastAPI **Hot-Reload Model tức thời**. |

---

## 6. BA VIỆC CẦN LÀM TIẾP THEO ĐỂ HOÀN THÀNH ĐỒ ÁN

1. **Việc 1 — Bổ sung script Distributed EDA bằng Spark SQL (`src/jobs/eda_sparksql.py`):**
   * Đây là nội dung cốt lõi của môn Big Data Analytics (tương tự bài tập `exe2`): Dùng Spark SQL truy vấn trên cụm để tính độ thưa ma trận ($99.74\%$), phân bố đuôi dài (Long-Tail), thống kê tương tác theo thể loại và xuất các biểu đồ trực quan vào `report/figures/`.
2. **Việc 2 — Khởi động và kiểm chứng toàn diện cụm HDFS + Streaming (End-to-End Demo Run):**
   * Chạy cụm với profile HDFS: `COMPOSE_PROFILES=hdfs docker compose -f docker/docker-compose.yml up -d --build`.
   * Kiểm tra giao diện Web HDFS NameNode (`http://localhost:9870`) và Spark Master UI (`http://localhost:8080`).
   * Chạy script phát luồng `python scripts/simulate_bulk_stream.py` để chứng minh luồng dữ liệu đi mượt mà từ Kafka $\rightarrow$ Spark Streaming $\rightarrow$ Delta Lake $\rightarrow$ Tự động Retrain ALS $\rightarrow$ Web UI cập nhật điểm gợi ý.
3. **Việc 3 — Đồng bộ hóa Báo cáo và Slide thuyết trình:**
   * Cập nhật các sơ đồ kiến trúc trong [`report/baocao_kientruc_pipeline.html`](file:///D:/learning/bigdata/final/report/baocao_kientruc_pipeline.html) và [`report/baocao_kientruc_pipeline.md`](file:///D:/learning/bigdata/final/report/baocao_kientruc_pipeline.md) để khớp hoàn toàn 100% với mã nguồn thực tế (HDFS + Kafka + Delta Lake + Spark Streaming vừa được triển khai thành công).

