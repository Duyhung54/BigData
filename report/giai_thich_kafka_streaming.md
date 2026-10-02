# Giải Thích Luồng Kafka & Streaming

> Mô tả code trên branch `kafka-streaming` (theo dõi `ThinhGH/BigData` → `Kafka,-Streaming-branch`, commit `c8a560d`).
> Phần 1 giải thích dễ hiểu bằng ví dụ đời thường. Phần 2 đi vào chi tiết kỹ thuật. Phần 3 liệt kê các điểm cần biết trước khi chạy hoặc trình bày.

---

# PHẦN 1 — GIẢI THÍCH DỄ HIỂU

## Ý tưởng chính trong một câu

> **Bạn chấm điểm một bộ phim trên web → vài phút sau, danh sách phim gợi ý cho bạn tự thay đổi theo.**

Trước đây, muốn gợi ý thay đổi phải chạy lại cả pipeline (~80 phút trên ml-25m). Phần streaming thêm một "đường tắt" để rating mới được học tự động.

## Hình dung như một quán ăn

| Trong quán ăn | Trong hệ thống | Làm gì |
|---|---|---|
| 🧾 Khách viết **phiếu góp ý** | Người dùng chấm điểm trên web | Tạo ra một rating mới |
| 📮 **Hộp đựng phiếu** ở quầy | **Kafka** | Giữ tạm phiếu, không để rơi mất |
| 🏃 **Nhân viên gom phiếu** mỗi 10 giây | **Spark Streaming** (`stream-ingest`) | Lấy phiếu trong hộp ra, chép vào sổ |
| 📒 **Sổ ghi chép** | **Delta Lake** | Lưu các rating mới, ghi đến đâu chắc đến đó |
| 📚 **Kho sổ cũ** | `ratings.parquet` | 25 triệu rating lịch sử |
| 👨‍🍳 **Đầu bếp** mỗi phút xem sổ một lần | **Huấn luyện lại ALS** (`train-als-stream`) | Đọc cả sổ cũ lẫn sổ mới, nghĩ lại món nên gợi ý cho từng khách |
| 📋 **Bảng thực đơn** treo tường | `recs.sqlite` | Danh sách gợi ý đã tính sẵn |
| 👀 Khách nhìn bảng | Trang web | Tải lại trang là thấy gợi ý mới |

## Một rating đi qua hệ thống như thế nào

Ví dụ: **user 1 chấm phim *Toy Story* 5 sao.**

```
 1. Bấm nút "Gửi vào Kafka" trên web
        │
        ▼
 2. Web đóng gói thành một tin nhắn:
    { user: 1, phim: Toy Story, điểm: 5, lúc: 14:00:03 }
    rồi bỏ vào "hộp thư" Kafka
        │
        ▼   (chờ tối đa 10 giây)
 3. Spark Streaming ghé qua, lấy hết tin nhắn mới,
    chép vào sổ Delta Lake
        │
        ▼   (chờ tối đa 60 giây)
 4. Bộ huấn luyện thức dậy:
    "Sổ có thêm dòng mới không?" → Có!
    → Gộp 25 triệu rating cũ + rating mới
    → Học lại từ đầu xem user 1 thích gì
    → Ghi danh sách gợi ý mới ra recs.sqlite
        │
        ▼
 5. Web phát hiện file recs.sqlite vừa đổi
    → nạp lại → user 1 bấm F5 thấy phim hoạt hình được gợi ý nhiều hơn
```

## Docker chia việc thế nào

Hình dung Docker là **một tòa nhà**, mỗi container là **một phòng**, mỗi phòng có **một nhân viên làm đúng một việc**:

```
┌─────────────────────── TÒA NHÀ DOCKER ───────────────────────┐
│                                                              │
│  📬 PHÒNG THƯ                 ⚙️ PHÒNG MÁY TÍNH              │
│   zookeeper  (quản lý)         spark-master  (phân việc)     │
│   kafka      (hộp thư)         spark-worker ×2 (làm việc)    │
│                                                              │
│  🤖 HAI "TRỢ LÝ" TỰ ĐỘNG       🌐 QUẦY TIẾP KHÁCH            │
│   stream-ingest                app  (trang web, cổng 8000)   │
│   → gom tin nhắn mỗi 10s                                     │
│   train-als-stream                                           │
│   → học lại mỗi 60s                                          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

Hai "trợ lý" `stream-ingest` và `train-als-stream` không tự tính toán. Chúng **giao việc** cho phòng máy (`spark-worker`) làm, rồi chờ kết quả.

Khi chạy mặc định (có HDFS), tòa nhà có thêm 3 phòng **kho lưu trữ**: 1 `namenode` làm thủ kho giữ sổ sách, 2 `datanode` làm kệ hàng chứa dữ liệu.

## Vì sao cần từng món

**Vì sao cần Kafka, sao không ghi thẳng?**
Hình dung 10.000 người chấm điểm cùng lúc. Nếu ai cũng chạy thẳng vào bếp thì bếp sẽ quá tải. Kafka là **cái hộp thư**: mọi người bỏ phiếu vào hộp rồi đi, bếp rảnh lúc nào thì lấy lúc đó. *(Riêng bản này: phiếu chỉ được giữ 30 phút, quá thời gian đó mà chưa ai lấy thì bị bỏ.)*

**Vì sao gom mỗi 10 giây, không lấy từng cái một?**
Như **xe buýt chạy theo chuyến**: chờ 10 giây gom một xe đầy rồi chạy, rẻ hơn nhiều so với mỗi người một chiếc taxi. Gọi là **micro-batch**.

**Vì sao dùng Delta Lake thay vì file thường?**
Delta Lake là sổ có **nhật ký ghi chép**: mỗi lần ghi hoặc là xong trọn vẹn, hoặc coi như chưa ghi. Nhờ vậy đầu bếp đọc sổ đúng lúc nhân viên đang chép cũng không bao giờ đọc phải một dòng viết dở.

## Tóm lại

```
Web/Script ──► Kafka ──► Spark Streaming ──► Delta Lake ──┐
 (chấm điểm)   (hộp thư)  (gom mỗi 10s)      (sổ mới)     │
                                                         ├──► Học lại ALS ──► recs.sqlite ──► Web
                                  ratings.parquet ───────┘    (mỗi 60s)       (bảng gợi ý)    (F5)
                                  (sổ cũ 25 triệu)
```

**Phần "streaming" là khâu nhận dữ liệu** (Kafka + Spark Streaming + Delta). **Khâu học vẫn là học lại theo lô**, chỉ được lặp tự động mỗi phút.

---

# PHẦN 2 — CHI TIẾT KỸ THUẬT

## 2.1 Tổng quan

Pipeline batch cũ (`ingest → train_als → evaluate → export_recs`) **vẫn giữ nguyên và vẫn phải chạy trước một lần**: nó tạo ra `ratings.parquet` (dữ liệu lịch sử) và `recs.sqlite`. Phần streaming thêm một vòng lặp chạy song song:

```
người dùng chấm điểm → Kafka → Spark Streaming → Delta Lake → train lại ALS → recs.sqlite → web
```

## 2.2 Docker: các container

> **Đã cập nhật:** trước đây có hai file (`docker-compose.yml` và `docker-compose-hdfs.yml`, file sau là bản chép của file trước). Nay gộp thành **một** `docker/docker-compose.yml`: 3 container HDFS nằm trong profile `hdfs`, và `docker/.env` bật sẵn profile này cùng `USE_HDFS=true`. Chạy mặc định có HDFS (11 container); muốn chạy không HDFS (8 container) thì gõ `COMPOSE_PROFILES= USE_HDFS=false docker compose -f docker/docker-compose.yml up -d`.

### Phần chung — 8 container

| Container | Image | Vai trò | Kiểu chạy |
|---|---|---|---|
| `spark-master` | movielens-spark | Spark Master, cấp tài nguyên | luôn chạy |
| `spark-worker` × 2 | movielens-spark | Executor, nơi tính toán thật | luôn chạy |
| `zookeeper` | `cp-zookeeper:7.6.1` | Quản lý metadata của Kafka | luôn chạy |
| `kafka` | `cp-kafka:7.6.1` | Message broker, giữ topic `ratings` | luôn chạy |
| `stream-ingest` | movielens-spark | **Spark driver** của job streaming, chạy mãi | app dài hạn |
| `train-als-stream` | movielens-spark | Vòng lặp bash: `spark-submit` → nghỉ 60 giây → lặp lại | app định kỳ |
| `app` | Dockerfile.app | FastAPI và **Kafka producer** | luôn chạy |

`stream-ingest` và `train-als-stream` là hai **driver**: chúng nộp job vào `spark-master`, phần tính toán thật chạy trên 2 worker.

Hai container này dùng `--packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`, nghĩa là **mỗi lần khởi động sẽ tải jar Delta và Kafka connector từ Maven** về `/tmp/.ivy2`. Thư mục này không được giữ lại, nên container khởi động lại là tải lại từ đầu, và cần có mạng.

### Thêm khi bật HDFS (mặc định) — 3 container

`namenode`, `datanode1`, `datanode2` (image `bde2020`, Hadoop 3.2.1, profile `hdfs`), và các container Spark nhận `USE_HDFS=true`. Khi đó Parquet, Delta và model lên HDFS (`src/config.py` ghép tiền tố `hdfs://namenode:9000`); `recs.sqlite`, `item_factors.npy`, `item_index.parquet` vẫn ở local. Nạp dữ liệu lần đầu bằng `scripts/migrate_to_hdfs.bat`.

## 2.3 Luồng chạy từng bước

```
  ① NGUỒN SỰ KIỆN
  ┌──────────────────┐      ┌──────────────────────────────┐
  │ Web UI :8000     │      │ simulate_bulk_stream.py      │  ← chạy trên máy host
  │ form "Gửi vào    │      │ 50 / 500 / không giới hạn    │
  │ Kafka"           │      │ rating mỗi giây              │
  └────────┬─────────┘      └──────────────┬───────────────┘
           │ POST /api/rate                 │
  ┌────────▼─────────┐                      │ localhost:29092
  │ FastAPI (app)    │ KafkaProducer        │
  │ gắn timestamp    ├──── kafka:9092 ──┐   │
  └──────────────────┘                  ▼   ▼
                          ② ┌──────────────────────────────┐
                            │ KAFKA · topic "ratings"      │ ← Zookeeper quản lý
                            │ giữ tối đa 30 phút / 200 MB  │
                            └──────────────┬───────────────┘
                                           │ kéo dữ liệu mỗi 10 giây
                          ③ ┌──────────────▼───────────────┐
                            │ stream-ingest                │ Spark Structured Streaming
                            │ JSON → bảng → ghi nối        │ chạy 24/7
                            └──────────────┬───────────────┘
                                           ▼
                          ④ ┌──────────────────────────┐   ┌────────────────────────┐
                            │ Delta Lake               │   │ ratings.parquet        │
                            │ lake/ratings_delta       │   │ (do pipeline batch tạo)│
                            │ = rating MỚI             │   │ = 25 triệu rating CŨ   │
                            └────────────┬─────────────┘   └───────────┬────────────┘
                                         └──────── union ──────────────┘
                                                    ▼
                          ⑤ ┌──────────────────────────────────────────────┐
                            │ train-als-stream  (vòng lặp, nghỉ 60 giây)   │
                            │  có rating mới? → fit ALS trên TOÀN BỘ dữ liệu│
                            │  → lưu model → VACUUM Delta → export_recs    │
                            └─────────────────────┬────────────────────────┘
                                                  ▼ ghi đè
                          ⑥ ┌──────────────────────────────────────────────┐
                            │ recs.sqlite ── API thấy file đổi (mtime) ──► │
                            │ nạp lại Store → người dùng F5 thấy gợi ý mới │
                            └──────────────────────────────────────────────┘
```

**① Nguồn sự kiện.**
- **Web:** form trong `serving/static/index.html` gọi `POST /api/rate`. Hàm `submit_rating()` trong `serving/api.py` gắn `timestamp = now` rồi đẩy JSON `{"userId", "movieId", "rating", "timestamp"}` vào topic `ratings`.
- **Giả lập:** `scripts/simulate_bulk_stream.py` có 4 chế độ: phát lại từ file CSV ở 50, 500 rating/giây hoặc không giới hạn tốc độ, và một chế độ tự sinh rating ngẫu nhiên 10 rating/giây. File được đọc **từng dòng** nên không tốn RAM.

**② Kafka** giữ và xếp hàng sự kiện. Producer gửi xong là quên, Spark đến lấy lúc nào tuỳ nó.

**③ `src/jobs/stream_ingest.py`:**
```python
spark.readStream.format("kafka").option("subscribe", "ratings")          # đọc luồng
  → CAST(value AS STRING) → from_json(..., RATING_SCHEMA)                 # bytes → JSON → cột
  → writeStream.format("delta").outputMode("append")                      # ghi nối
    .trigger(processingTime="10 seconds")                                 # micro-batch 10s
    .option("checkpointLocation", ...)                                    # nhớ đã đọc tới đâu
```

**④ Delta Lake** = Parquet + thư mục `_delta_log/` chứa nhật ký giao dịch. Mỗi micro-batch là một commit nguyên tử, nên `train-als-stream` đọc trong lúc `stream-ingest` đang ghi cũng không thấy dữ liệu dở dang.

**⑤ `src/jobs/train_als_stream.py`**, mỗi vòng lặp:
1. Đếm số dòng trong Delta. Nếu **bằng** số đã lưu ở `data/output/.last_trained_stream_count` thì bỏ qua lượt này.
2. `historical.unionByName(stream)`: gộp rating cũ với rating mới.
3. Đọc `rank`, `regParam` từ `report/results/best_params.csv`, **fit ALS đúng một lần** (không chạy lại grid search 15 tổ hợp).
4. Lưu model, chạy `VACUUM` dọn file cũ của Delta, rồi gọi `export_recs.main()` để ghi lại `recs.sqlite`.

**⑥ Hot-reload.** Mỗi request, `get_store()` trong `serving/api.py` so `mtime` của `recs.sqlite`. Nếu file đã đổi, API nạp lại `Store` mà không cần khởi động lại server.

## 2.4 Kafka hoạt động thế nào

| Khái niệm | Trong code này |
|---|---|
| **Topic** | `ratings`, tự tạo khi có message đầu tiên (`KAFKA_AUTO_CREATE_TOPICS_ENABLE`) |
| **Producer** | FastAPI (`kafka-python`) và các script giả lập |
| **Consumer** | Spark Structured Streaming trong `stream-ingest` |
| **Offset** | Vị trí đã đọc tới; Spark lưu trong `checkpointLocation` |
| **Retention** | Xoá message sau **30 phút** hoặc khi vượt **200 MB** |
| **Zookeeper** | Kafka 7.6 bản này vẫn cần Zookeeper giữ metadata cụm |

**Vì sao có hai cổng:**
```
INTERNAL://kafka:9092       ← container nói chuyện với nhau (app, stream-ingest)
EXTERNAL://localhost:29092  ← script Python chạy trên máy Windows
```
Kafka trả về cho client **địa chỉ mà client phải dùng để kết nối lại**. Container hiểu tên `kafka`, máy host thì không, nên phải khai báo hai "danh tính".

Cụm này có **1 broker, 1 partition, replication 1**: có **cơ chế** Kafka, nhưng không có song song theo partition và không chịu lỗi nếu broker chết.

## 2.5 Streaming xử lý thế nào — điểm quan trọng nhất

**Phần nhận dữ liệu là streaming thật** (micro-batch 10 giây, có checkpoint, ghi vào Delta theo kiểu exactly-once).

**Phần học máy thì không streaming.** Spark MLlib ALS **không cập nhật được từng phần** (không có `partial_fit`). Mỗi vòng, `train-als-stream` **huấn luyện lại từ đầu trên toàn bộ** dữ liệu cũ cộng phần mới. Nói chính xác:

> **Thu thập theo luồng (micro-batch) + huấn luyện lại định kỳ theo lô.**

Hệ quả:
- **Không phải "F5 là thấy ngay".** Độ trễ = tối đa 10 giây vào Delta + tối đa 60 giây chờ vòng lặp + thời gian fit ALS + thời gian `export_recs`. Trên ml-25m, `export_recs` phải sinh gợi ý cho 162.541 user và ghi SQLite, nên mất **vài phút**. Trên `ml-latest-small` thì nhanh hơn nhiều.
- **Lambda hay Kappa?** `report/streaming_architecture_report.md` gọi đây là *Kappa Architecture*. Kappa là *một* luồng duy nhất, xử lý lại mọi thứ từ log. Ở đây có hai kho riêng (Parquet lịch sử và Delta mới) được gộp lại để train theo lô, nên gần với **Lambda** hơn.

## 2.6 Phần Big Data nằm ở đâu

| Công nghệ | Vai trò |
|---|---|
| **Kafka** | Log phân tán, tách nơi sinh dữ liệu khỏi nơi xử lý |
| **Spark Structured Streaming** | Xử lý luồng micro-batch, checkpoint offset |
| **Delta Lake** | Giao dịch ACID trên Parquet: đọc và ghi đồng thời an toàn, `VACUUM` dọn file |
| **Spark MLlib ALS** | Huấn luyện lại trên dữ liệu gộp |
| **HDFS** (bản `-hdfs`) | Lưu Parquet, Delta và model phân tán |

---

# PHẦN 3 — ĐIỂM CẦN BIẾT TRƯỚC KHI CHẠY HOẶC TRÌNH BÀY

> **Trạng thái sửa (chưa commit, chưa chạy thử):** đã sửa mục **1** (`stream-ingest` giới hạn `spark.cores.max=2`), mục **2** (`reset_demo.bat` chỉ xoá `ratings_delta` và nạp lại dữ liệu sau khi dựng cụm), mục **3** (`api.py` dùng `Optional[...]`), mục **10** (gộp còn một file compose). Thêm hai sửa không có trong bảng: `item_index.parquet` luôn ghi ở local cho khớp chỗ web đọc, và `train-als-stream` được đặt `SPARK_MASTER_URL` để chạy trên cụm thay vì `local[*]`. Các mục còn lại (4, 5, 6, 7, 8, 9) giữ nguyên.

## Nói dễ hiểu: 3 điều quan trọng nhất

**1. "Học lại từ đầu", không phải "học thêm".**
Mỗi lần có rating mới, ALS **học lại toàn bộ** dữ liệu. Giống như mỗi lần có một phiếu góp ý, đầu bếp phải đọc lại **cả kho sổ** từ đầu. Vì vậy với bộ dữ liệu lớn, gợi ý thay đổi sau **vài phút**, không phải ngay lập tức.

**2. Có thể bị "kẹt máy".**
Nhân viên gom phiếu chạy suốt ngày và mặc định **chiếm hết 6 core** của phòng máy. Đầu bếp đến thì không còn chỗ, phải đứng chờ mãi. Cách sửa: giới hạn `stream-ingest` dùng tối đa 2 core. *(Suy ra từ cách Spark standalone cấp tài nguyên mặc định, chưa chạy thử để xác nhận.)*

**3. ⚠️ Đừng chạy `scripts/reset_demo.bat` khi chưa sao lưu.**
Script này xoá luôn **kho sổ cũ** (`data\lake`, gồm `ratings.parquet`) và **bảng thực đơn** (`recs.sqlite`). Chạy xong thì đầu bếp không còn gì để học, demo hỏng, muốn phục hồi phải chạy lại pipeline batch.

## Bảng chi tiết

| # | Vấn đề | Hậu quả | Cách kiểm hoặc sửa |
|---|---|---|---|
| 1 | **Không giới hạn core cho từng app Spark** (không có `spark.cores.max` hay `--total-executor-cores` ở đâu trong repo) | Ở chế độ standalone, mỗi app mặc định chiếm hết core trống. `stream-ingest` chạy mãi và giữ cả 6 core, nên rất có thể `train-als-stream` bị kẹt ở trạng thái **WAITING** | Xem http://localhost:8080. Sửa: thêm `--total-executor-cores 2` vào lệnh của `stream-ingest` |
| 2 | **`reset_demo.bat` xoá cả `data\lake`**, không chỉ Delta | Mất `ratings.parquet` và `recs.sqlite`; `train-als-stream` không còn dữ liệu lịch sử để train | Sao lưu trước khi chạy, hoặc sửa script chỉ xoá `data\lake\ratings_delta` |
| 3 | `serving/api.py` dòng 58 có `sqlite3.Connection \| None`, file không có `from __future__ import annotations` | Container `app` (Python 3.11) chạy bình thường, nhưng **test chạy trong container Spark (Python 3.8) lỗi ngay lúc import `api.py`** | Đổi thành `Optional[sqlite3.Connection]` |
| 4 | Endpoint `/recommendations/{user_id}` trỏ tới `/opt/app/data/output/model/recs.sqlite` (sai đường dẫn) và truy vấn cột `rating`, trong khi bảng dùng cột `score` | Gọi endpoint này lỗi 500. Giao diện không dùng nó nên chưa bị phát hiện | Xoá endpoint, hoặc sửa đường dẫn và tên cột |
| 5 | Đường dẫn cứng `E:\BigData\…` trong `generate_stream_test_data.py`, `plot_trend.py`, `simulate_bulk_stream.py` | Trên máy khác ổ hoặc thư mục, các script không tìm thấy file | `simulate_bulk_stream.py` nhận đường dẫn qua tham số thứ hai; hai script còn lại phải sửa code |
| 6 | `startingOffsets="latest"` kết hợp retention 30 phút | "Không mất dòng nào" chỉ đúng **khi** `stream-ingest` đã chạy trước và không ngừng quá 30 phút. Message gửi trước lần khởi động đầu tiên bị bỏ qua | Nêu đúng phạm vi đảm bảo trong báo cáo |
| 7 | `scripts/produce_ratings.py` sinh `userId ≤ 6040`, `movieId ≤ 3952` (khoảng ID của bộ ml-1m) | Nhiều `movieId` sinh ra không có trong ml-25m | Dùng `simulate_bulk_stream.py` thay thế |
| 8 | Commit `75255fc` xoá `report/results/metrics.csv`, `scaling.csv` và thay `best_params.csv` | Merge vào `main` sẽ mất số liệu ml-25m đang làm bằng chứng cho báo cáo | Không merge commit đó, hoặc khôi phục các file này trước khi merge |
| 9 | Có hai bản tích hợp HDFS khác nhau: branch này (`USE_HDFS`, đưa Parquet + Delta + model lên HDFS, bỏ qua đo dung lượng nên `parquet_bytes = 0`) và branch `feat/hdfs` (`LAKE_ROOT`, chỉ đưa lake lên HDFS, đo dung lượng thật) | Cả hai cùng sửa `src/config.py`, `src/jobs/ingest.py`, `docker/docker-compose.yml`, nên không merge thẳng được | Chọn một cách, hoặc kết hợp có chủ đích |
| 10 | Compose của branch này dùng cùng tên container `namenode`, `spark-master`, `movielens-api` với cụm của `feat/hdfs` | Chạy hai cụm cùng lúc sẽ báo lỗi trùng tên | Tắt cụm này trước khi bật cụm kia |

Mục 1 là suy luận từ hành vi mặc định của Spark standalone, chưa chạy thử để xác nhận. Các mục còn lại đọc trực tiếp từ code.
