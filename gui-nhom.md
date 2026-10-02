# TÀI LIỆU KỸ THUẬT & KẾ HOẠCH TÁI CẤU TRÚC HỆ THỐNG BIG DATA
## Project: MovieLens Distributed Recommendation & Analytics System
### Mục đích: Định hướng kỹ thuật để mô hình/lập trình viên thực hiện sửa mã nguồn

---

## 1. ĐẶC TẢ KIẾN TRÚC MỤC TIÊU (TARGET ARCHITECTURE)

Hệ thống tuân thủ mô hình **Lambda Architecture** phân tách rõ ràng giữa hai luồng xử lý:

```
[Nguồn dữ liệu / Client]
         │
         ▼
[Kafka Broker: Topic "user_events"]
    │                   │
    │ (Luồng Stream)    │ (Lưu trữ dữ liệu thô)
    ▼                   ▼
[Spark Streaming]   [HDFS NameNode]
    │                   │
    ▼ (10s micro-batch) ├── [DataNode 1] ── /raw/
[Redis In-Memory]       ├── [DataNode 2] ── /processed/
    │                   └── [DataNode 3] ── /models/
    ▼ (Serving)                 │
[Top Trending 1h]               ▼ (Batch định kỳ)
    ▲               [Spark Batch: Huấn luyện ALS]
    │                           │
    └───────────┬───────────────┘
                ▼
      [PostgreSQL Database]
                │
                ▼
     [FastAPI Serving Layer]
```

### Nguyên tắc vận hành từng luồng:
1. **Luồng Stream (Tốc độ / Thời gian thực)**:
   * **Nguồn**: Sự kiện tương tác thời gian thực từ Kafka topic `user_events`.
   * **Xử lý**: Spark Structured Streaming đọc với micro-batch (10 giây) $\rightarrow$ Thực hiện phép tính toán trượt cửa sổ thời gian (Sliding Window Aggregation) đếm tần suất tương tác trong 1 giờ gần nhất.
   * **Đích đến**: Ghi kết quả "Top phim thịnh hành 1h" vào **Redis In-Memory** để phục vụ API truy vấn độ trễ $< 5$ms.
   * **Lưu ý**: Luồng Stream **không** thực hiện huấn luyện lại mô hình học máy.
2. **Luồng Batch (Khối / Toàn cục)**:
   * **Nguồn**: Dữ liệu lịch sử lưu trên HDFS + dữ liệu sự kiện tích lũy trong Delta Lake/HDFS.
   * **Xử lý**: Job Spark Batch chạy định kỳ $\rightarrow$ Đọc dữ liệu Parquet đã chuẩn hóa $\rightarrow$ Huấn luyện mô hình lọc cộng tác Spark MLlib ALS và mô hình Content-Based Filtering.
   * **Đích đến**: Xuất kết quả gợi ý cá nhân hóa Top-10 vào **PostgreSQL**.

---

## 2. ĐÁNH GIÁ KỸ THUẬT HIỆN TRẠNG MÃ NGUỒN (AUDIT)

| Thành phần | Hiện trạng trong mã nguồn | Đánh giá kỹ thuật | Yêu cầu sửa đổi |
| :--- | :--- | :--- | :--- |
| **Cụm HDFS** | `docker-compose.yml` đang cấu hình 2 DataNodes, `dfs.replication = 2`, `dfs.blocksize = 128MB`. | Chưa đạt chuẩn cấu hình chịu lỗi 3 bản sao của Hadoop. | Nâng lên **3 DataNodes**, cấu hình `dfs.replication = 3`, `dfs.blocksize = 268435456` (256 MB). |
| **Khai thác dữ liệu** | Mới chỉ nạp và xử lý 2/6 tệp (`ratings.csv` và `movies.csv`). | Bỏ phí 4 tệp dữ liệu, đặc biệt là `genome-scores.csv` (15.58M bản ghi). | Tích hợp thêm `genome-scores.csv` và `tags.csv` để xây dựng vector đặc trưng 1.128 chiều cho Content-Based. |
| **Xử lý Stream** | `train_als_stream.py` chạy vòng lặp 60s gọi lại `ALS.fit()` trên toàn bộ dữ liệu. | Sai nguyên lý vận hành Big Data; huấn luyện lại ma trận 25M dòng sau mỗi 60s gây nghẽn CPU và tràn đĩa đệm. | Tách luồng Stream thành 2 nhiệm vụ: (1) Append dữ liệu vào Delta Lake; (2) Tính toán sliding window Top trending lưu vào Redis. Bỏ vòng lặp retrain ALS trong stream. |
| **Tầng lưu trữ Serving** | Đang dùng tệp SQLite cục bộ (`recs.sqlite`). | Chưa đáp ứng môi trường phân tán chuẩn công nghiệp. | Thay thế bằng **PostgreSQL** cho danh sách gợi ý và **Redis** cho dữ liệu thời gian thực. |

---

## 3. CẤU HÌNH HẠ TẦNG DOCKER COMPOSE MỤC TIÊU (`docker/docker-compose.yml`)

```yaml
version: "3.8"

services:
  # --- CỤM HDFS (1 NameNode + 3 DataNodes) ---
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    restart: always
    ports:
      - "9870:9870"
      - "9000:9000"
    environment:
      - CLUSTER_NAME=movielens-cluster
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
      - HDFS_CONF_dfs_replication=3
      - HDFS_CONF_dfs_blocksize=268435456 # 256 MB
      - HDFS_CONF_dfs_permissions_enabled=false
    volumes:
      - namenode_data:/hadoop/dfs/name

  datanode1:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode1
    restart: always
    depends_on:
      - namenode
    environment:
      - SERVICE_PRECONDITION=namenode:9870
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    volumes:
      - datanode1_data:/hadoop/dfs/data

  datanode2:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode2
    restart: always
    depends_on:
      - namenode
    environment:
      - SERVICE_PRECONDITION=namenode:9870
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    volumes:
      - datanode2_data:/hadoop/dfs/data

  datanode3:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode3
    restart: always
    depends_on:
      - namenode
    environment:
      - SERVICE_PRECONDITION=namenode:9870
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    volumes:
      - datanode3_data:/hadoop/dfs/data

  # --- CỤM SPARK ---
  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      - SPARK_MODE=master
    volumes:
      - ./src:/opt/app/src
      - ./data:/opt/data

  spark-worker:
    image: bitnami/spark:3.5
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
    deploy:
      replicas: 2
    volumes:
      - ./src:/opt/app/src
      - ./data:/opt/data

  # --- MESSAGE BROKER (KAFKA) ---
  kafka:
    image: bitnami/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,EXTERNAL://:29092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER

  # --- SERVING DATABASES ---
  redis:
    image: redis:7-alpine
    container_name: redis
    ports:
      - "6379:6379"

  postgres:
    image: postgres:15-alpine
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=movielens_db
      - POSTGRES_USER=admin
      - POSTGRES_PASSWORD=adminpassword
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  namenode_data:
  datanode1_data:
  datanode2_data:
  datanode3_data:
  postgres_data:
```

---

## 4. NGUYÊN TẮC KỸ THUẬT: BLOCKSIZE VÀ ĐỊNH DẠNG DỮ LIỆU

### 4.1 Quy tắc BlockSize 256 MB
* Kích thước tệp CSV thô: $\approx 2 \text{ GB}$ (2048 MB).
* Số lượng khối mục tiêu: $2048 \text{ MB} / 256 \text{ MB} = 8 \text{ blocks}$.
* Khi áp dụng `Replication = 3`: Hệ thống sinh ra $8 \times 3 = 24 \text{ blocks}$ phân bổ đều sang 3 DataNodes (mỗi DataNode lưu 8 blocks).
* **Mục tiêu kỹ thuật**: Giảm thiểu chi phí quản lý Metadata trên bộ nhớ RAM của NameNode và tối ưu thông lượng I/O đọc tuần tự cho Spark.

### 4.2 Xử lý mâu thuẫn giữa nén Snappy và phân chia khối
* Khi nén CSV sang Snappy, dung lượng tệp co lại còn $\approx 200 \text{ MB}$. Nếu lưu trữ thành 1 tệp `.snappy` đơn lẻ, HDFS sẽ chỉ cấp phát 1 block duy nhất do kích thước nhỏ hơn BlockSize 256 MB $\rightarrow$ Mất tính xử lý song song trên nhiều CPU core của Spark.
* **Giải pháp kỹ thuật bắt buộc**: Chuyển đổi dữ liệu sang định dạng **Apache Parquet nén Snappy**:
  * Parquet tổ chức dữ liệu theo cột và chia nhỏ thành các Row-groups ($\approx 128 \text{ MB}$).
  * Thực hiện ghi dữ liệu với phân vùng tường minh qua Spark:
    ```python
    df = spark.read.csv("hdfs://namenode:9000/raw/ratings.csv", header=True, inferSchema=False)
    df.repartition(4).write.mode("overwrite").parquet(
        "hdfs://namenode:9000/processed/ratings_parquet",
        compression="snappy"
    )
    ```

---

## 5. DANH MỤC NHIỆM VỤ TÁI CẤU TRÚC MÃ NGUỒN (ACTIONABLE TASKS)

### Nhiệm vụ 1: Cập nhật tệp cấu hình hạ tầng
* **File cần sửa**: `docker/docker-compose.yml`.
* **Nội dung sửa**:
  1. Thêm service `datanode3`.
  2. Bổ sung cấu hình `HDFS_CONF_dfs_replication=3` và `HDFS_CONF_dfs_blocksize=268435456` trong service `namenode`.
  3. Thêm service `postgres:15-alpine` và `redis:7-alpine`.

### Nhiệm vụ 2: Chuẩn hóa tham số cấu hình hệ thống
* **File cần sửa**: `src/config.py`.
* **Nội dung sửa**:
  1. Đặt `HDFS_REPLICATION = 3`.
  2. Đặt `HDFS_BLOCKSIZE = 268435456`.
  3. Bổ sung các biến kết nối PostgreSQL (`POSTGRES_URI`) và Redis (`REDIS_HOST`, `REDIS_PORT`).
  4. Bổ sung đường dẫn HDFS cho các tệp mở rộng: `GENOME_SCORES_CSV`, `TAGS_CSV`, `GENOME_SCORES_PARQUET`.

### Nhiệm vụ 3: Tái cấu trúc pipeline ETL Ingestion
* **File cần sửa**: `src/jobs/ingest.py`.
* **Nội dung sửa**:
  1. Nạp toàn bộ 6 tệp từ HDFS `/raw/` thay vì chỉ 2 tệp.
  2. Chuyển đổi `ratings.csv` và `genome-scores.csv` sang Parquet nén Snappy với số partition tối thiểu bằng số Core của cụm (`repartition(4)` hoặc `repartition(8)`).
  3. Ghi trực tiếp vào `hdfs://namenode:9000/processed/`.

### Nhiệm vụ 4: Tái cấu trúc luồng xử lý Stream (Loại bỏ Retrain)
* **File cần sửa**: `src/jobs/stream_ingest.py` và loại bỏ vòng lặp trong `src/jobs/train_als_stream.py`.
* **Nội dung sửa**:
  1. Cấu hình `stream_ingest.py` đọc từ Kafka topic `user_events`.
  2. Thực hiện ghi append vào Delta Lake `/processed/ratings_delta` với trigger micro-batch 10s.
  3. Thêm nhánh xử lý windowing:
     ```python
     trending_df = (
         ratings_stream
         .groupBy(window(col("timestamp"), "1 hour", "10 minutes"), col("movieId"))
         .count()
         .orderBy(col("count").desc())
         .limit(20)
     )
     ```
  4. Ghi trực tiếp bảng kết quả đếm Top 20 vào Redis key `trending:movies:1h`.
  5. Xóa bỏ hoàn toàn logic huấn luyện lại ALS định kỳ mỗi 60 giây trong luồng stream.

### Nhiệm vụ 5: Tái cấu trúc luồng xử lý Batch & Tích hợp PostgreSQL
* **File cần sửa**: `src/jobs/train_als.py` và `src/jobs/export_recs.py`.
* **Nội dung sửa**:
  1. Giữ nguyên logic chia Temporal Split và Grid Search của ALS.
  2. Sửa hàm xuất dữ liệu trong `export_recs.py`: Ghi Top-10 gợi ý cho từng người dùng vào bảng `recommendations` trong PostgreSQL thông qua JDBC (`org.postgresql.Driver`).

### Nhiệm vụ 6: Xây dựng mô-đun Feature Engineering & Content-Based Filtering
* **File cần tạo mới**: `src/jobs/train_content_based.py`.
* **Nội dung thực hiện**:
  1. Đọc tệp `genome-scores.csv` (15.58M bản ghi).
  2. Chuẩn hóa ma trận đặc trưng $13.816 \times 1.128$ thành vector embedding cho từng bộ phim (`movie_embedding`).
  3. Tính toán ma trận tương đồng Cosine Similarity giữa các bộ phim.
  4. Xuất bảng phim tương đồng vào PostgreSQL phục vụ bài toán Cold-Start và tính năng giải thích gợi ý.

### Nhiệm vụ 7: Cập nhật Tầng API Serving
* **File cần sửa**: `serving/api.py`.
* **Nội dung sửa**:
  1. Bổ sung endpoint `GET /api/trending`: Đọc dữ liệu từ Redis key `trending:movies:1h`.
  2. Sửa endpoint `GET /api/users/{user_id}/recommendations`: Truy vấn danh sách Top-10 từ bảng `recommendations` trong PostgreSQL.
  3. Endpoint `POST /api/rate`: Gửi sự kiện JSON vào Kafka topic `user_events`.
