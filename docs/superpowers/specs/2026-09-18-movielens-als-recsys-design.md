# Thiết kế: Hệ thống gợi ý phim trên MovieLens với Spark MLlib ALS

- **Ngày:** 2026-09-18
- **Bối cảnh:** Đồ án cuối kỳ môn Big Data
- **Quy mô:** 1 người, 2–3 tuần
- **Trọng tâm chấm điểm:** demo chạy được + báo cáo; kiến trúc vừa phải

---

## 1. Mục tiêu và phạm vi

### 1.1 Mục tiêu

Xây dựng hệ thống gợi ý phim end-to-end trên bộ dữ liệu MovieLens 25M, gồm:

1. Pipeline xử lý dữ liệu phân tán bằng Spark (ingest → train → evaluate → export).
2. Mô hình collaborative filtering dùng thuật toán ALS của Spark MLlib, có tuning siêu tham số.
3. Đánh giá mô hình nghiêm túc: metrics dự đoán, metrics xếp hạng, và so sánh với baseline.
4. Tầng phục vụ trực tuyến (API + giao diện web) tách rời khỏi tầng huấn luyện.
5. Thí nghiệm đo khả năng mở rộng theo số worker.

### 1.2 Tuyên bố vấn đề

Bản thân việc gọi `ALS` từ Spark MLlib là vài chục dòng code. Giá trị của đồ án không nằm ở đó, mà ở ba chỗ:

- **Xử lý dữ liệu ở quy mô vượt khả năng của một tiến trình đơn**: 25 triệu bản ghi, thao tác định dạng cột, tránh lỗi small-files.
- **Đánh giá trung thực**: tách tập theo thời gian để không rò rỉ dữ liệu, và đối chiếu với baseline phổ biến (một mô hình thua baseline popularity là mô hình vô dụng).
- **Kiến trúc batch/serving tách rời**: đúng cách các hệ gợi ý thực tế vận hành.

### 1.3 Ngoài phạm vi (YAGNI)

Các hạng mục sau **cố ý không làm**, kèm lý do:

| Hạng mục | Lý do loại |
|---|---|
| HDFS | Một node HDFS không chứng minh thêm điều gì so với volume mount, tốn ~2 ngày cấu hình. |
| Kafka / Spark Streaming | ALS là thuật toán batch, không cập nhật online được. Streaming sẽ chỉ để trang trí và dễ lộ khi bị hỏi kỹ. |
| Airflow / Prefect | Một script `run_pipeline.sh` là đủ cho 4 job tuần tự. |
| Xác thực người dùng | Không liên quan tới nội dung môn học. |
| Poster phim từ TMDB API | Đẹp nhưng không đóng góp vào tiêu chí chấm. |
| Deep learning (NCF, two-tower) | Vượt phạm vi môn và vượt ngân sách thời gian. |

---

## 2. Kiến trúc tổng thể

```
┌─────────────────────────── TẦNG BATCH (Spark) ───────────────────────────┐
│                                                                          │
│  ml-25m/*.csv ──▶ ingest.py ──▶ data/lake/*.parquet                      │
│                                      │                                   │
│                                      ├──▶ train_als.py ──▶ model/        │
│                                      │                     tuning.csv    │
│                                      │                                   │
│                                      ├──▶ evaluate.py ──▶ metrics.csv    │
│                                      │                                   │
│                                      └──▶ export_recs.py ─▶ recs.sqlite  │
│                                                             item_factors │
└──────────────────────────────────────────────────────────────┬───────────┘
                                                               │
┌─────────────────────── TẦNG SERVING (FastAPI) ───────────────▼───────────┐
│  api.py  ──reads──▶ recs.sqlite (index trên userId)                      │
│          ──loads──▶ item_factors.npy (nạp vào RAM lúc khởi động)         │
│          ──serves─▶ static/index.html                                    │
└──────────────────────────────────────────────────────────────────────────┘
```

**Ràng buộc kiến trúc then chốt:** tầng serving **không bao giờ** khởi tạo SparkSession. Mỗi lần khởi tạo tốn 10–20 giây; nếu đặt trong đường xử lý request thì demo sẽ trông như treo. Ranh giới giữa hai tầng là hệ thống file: batch ghi ra, serving đọc vào.

---

## 3. Hạ tầng

### 3.1 Docker

| Service | Image | Tài nguyên |
|---|---|---|
| `spark-master` | `spark:3.5.x-python3` | 1 core, 1 GB |
| `spark-worker` (×2) | `spark:3.5.x-python3` | 3 core, 3 GB mỗi worker |
| `app` | `python:3.11-slim` | FastAPI, không cần Spark |

**Chọn image:** dùng Docker Official Image `spark`, **không** dùng `bitnami/spark`. Trong năm 2025 Bitnami đã dời các image miễn phí sang namespace `bitnamilegacy` và giới hạn tag; phần lớn tutorial Spark + Docker trên mạng viết trước đó nên pull về sẽ lỗi.

**Chọn phiên bản:** Spark 3.5.x thay vì 4.x — tài liệu và câu trả lời trên StackOverflow dày hơn đáng kể, quan trọng khi gặp lỗi lúc gấp.

### 3.2 Mount dữ liệu

Toàn bộ container mount `./data` vào **cùng một đường dẫn** `/opt/data`.

Đây là lỗi phổ biến nhất khi chạy Spark cluster: driver khai báo đường dẫn theo góc nhìn của nó, nhưng executor phải mở được đúng đường dẫn đó trên chính container của mình. Lệch path sẽ ra `FileNotFoundException` rất khó truy vết.

### 3.3 Cấu hình máy chủ (máy phát triển)

- 8 core vật lý, 16 GB RAM, ổ D: còn 185 GB.
- Docker Desktop trên WSL2 mặc định chỉ nhận ~50% RAM. **README phải hướng dẫn chỉnh `%USERPROFILE%\.wslconfig`** lên `memory=12GB`, nếu không executor sẽ bị OOM-kill giữa chừng mà không có thông báo rõ ràng.

### 3.4 Cấu trúc thư mục

```
BigData_Final/
├── docker/
│   ├── docker-compose.yml
│   └── Dockerfile
├── data/                       # .gitignore toàn bộ
│   ├── raw/                    # ml-25m/*.csv
│   ├── lake/                   # parquet
│   └── output/                 # model, recs, factors
├── src/
│   ├── config.py               # mọi đường dẫn & tham số tập trung một chỗ
│   ├── session.py              # khởi tạo SparkSession
│   ├── jobs/
│   │   ├── ingest.py
│   │   ├── train_als.py
│   │   ├── evaluate.py
│   │   └── export_recs.py
│   └── common/
│       ├── schema.py
│       └── metrics.py          # Precision@K / Recall@K / NDCG@K / Coverage
├── serving/
│   ├── api.py
│   └── static/index.html
├── tests/
├── scripts/
│   ├── download_data.sh
│   ├── run_pipeline.sh
│   └── scaling_experiment.sh
├── report/
│   ├── make_figures.py
│   ├── figures/
│   └── results/
├── requirements.txt
└── README.md
```

**Nguyên tắc tách module:** mỗi job trong `src/jobs/` là một chương trình độc lập, nhận input từ đĩa và ghi output ra đĩa, chạy được bằng `spark-submit` riêng lẻ. Nhờ vậy khi sửa logic đánh giá, không phải huấn luyện lại — với ALS trên 25 triệu rating, đó là khác biệt giữa chờ 30 giây và chờ 15 phút mỗi vòng lặp.

Phần logic tính toán cốt lõi trong `src/common/metrics.py` không phụ thuộc Spark, để unit test chạy được trong một giây.

---

## 4. Dữ liệu

### 4.1 Bộ dữ liệu

**MovieLens 25M** (`ml-25m`): khoảng 25.000.000 rating, ~62.000 phim, ~162.000 user. `ratings.csv` khoảng 650 MB.

Lý do chọn cỡ này: đủ lớn để pandas trên máy 16 GB bắt đầu khó khăn — tức là có lý do chính đáng để dùng Spark — nhưng vẫn huấn luyện xong trong vài phút.

Bộ `ml-latest-small` (~100.000 rating, 1 MB) dùng cho phát triển và cho integration test.

### 4.2 Job 1 — `ingest.py`

Chuyển CSV sang Parquet + nén Snappy.

Hai quyết định có chủ ý, đều là nội dung phân tích trong báo cáo:

1. **Khai báo schema tường minh, không dùng `inferSchema=True`.** `inferSchema` buộc Spark quét toàn bộ 650 MB thêm một lượt chỉ để suy ra kiểu dữ liệu. Job sẽ đo và ghi lại chênh lệch thời gian giữa hai cách.

2. **Gộp về khoảng 8–12 file Parquet (~128 MB/file), không partition theo `userId`.** Partition theo `userId` sẽ sinh ~162.000 thư mục con — lỗi small-files kinh điển, làm chậm nghiêm trọng thao tác đọc và tạo áp lực lên metadata. Vì mọi job phía sau đều đọc toàn bộ dữ liệu, partition theo cột không mang lại lợi ích gì.

**Đầu ra:** `data/lake/ratings.parquet`, `data/lake/movies.parquet`, và `report/results/ingest_stats.csv` (dung lượng trước/sau, thời gian đọc trước/sau).

### 4.3 Chiến lược chia tập

**Chia ba phần theo thời gian trong từng user:** với mỗi user, sắp xếp rating theo `timestamp` rồi cắt:

| Tập | Tỷ lệ | Dùng để |
|---|---|---|
| `train` | 70% cũ nhất | Huấn luyện mô hình |
| `validation` | 15% tiếp theo | Chọn siêu tham số (grid search) |
| `test` | 15% mới nhất | Báo cáo kết quả cuối cùng, **chỉ chạm vào một lần** |

**Vì sao phải có tập validation riêng.** Nếu chọn siêu tham số dựa trên chính tập test rồi lại báo cáo điểm trên tập test đó, con số báo cáo sẽ lạc quan hơn thực tế — mô hình đã được chọn để hợp với tập test. Đây là dạng rò rỉ tinh vi và rất phổ biến trong đồ án sinh viên. Tập test chỉ được đánh giá đúng một lần, sau khi đã chốt siêu tham số trên tập validation.

**Vì sao không dùng `randomSplit`.** `randomSplit` cho mô hình nhìn thấy đánh giá ở tương lai của một user rồi bắt nó dự đoán đánh giá ở quá khứ của chính user đó. Đây cũng là rò rỉ dữ liệu, và làm điểm số đẹp một cách giả tạo. Cách chia theo thời gian phản ánh đúng tình huống thực tế: dự đoán hành vi tương lai từ hành vi quá khứ.

Cài đặt: `Window.partitionBy("userId").orderBy("timestamp")` + `percent_rank()`, cắt tại 0.70 và 0.85.

**Ghi chú về user ít rating:** user có dưới ~5 rating sẽ không chia được thành ba phần có nghĩa. Các user này giữ nguyên toàn bộ trong tập train và bị loại khỏi phần đánh giá — số lượng user bị loại phải được ghi lại và nêu trong báo cáo, vì nó chính là biểu hiện của bài toán cold-start.

---

## 5. Mô hình

### 5.1 Job 2 — `train_als.py`

Thuật toán: ALS explicit feedback (`implicitPrefs=False`), vì MovieLens có điểm số tường minh 0.5–5.0.

**Lưới siêu tham số:**

| Tham số | Giá trị |
|---|---|
| `rank` | 10, 50, 100 |
| `regParam` | 0.01, 0.1, 0.2, 0.3, 0.5 |
| `maxIter` | 10 (cố định) |

**Vì sao `regParam` kéo tới 0.5.** Lần chạy thử trên `ml-latest-small` cho `regParam=0.2` thắng — nhưng 0.2 là giá trị lớn nhất trong lưới ban đầu, tức tối ưu nằm ở **biên**, và một kết quả tuning chạm biên là kết quả chưa kết luận được. "Sao không thử giá trị lớn hơn" là câu hỏi đầu tiên người chấm sẽ đặt. Thêm 0.3 và 0.5 để tối ưu nằm hẳn bên trong lưới.

Cùng lần chạy đó cho thấy **`rank` gần như không ảnh hưởng**: tại `regParam=0.2`, rank 10/50/100 cho RMSE 0.90823 / 0.90886 / 0.90891 — chênh 0.0007, tức nhiễu. Vẫn giữ cả ba rank vì "tăng số chiều ẩn không cải thiện gì, chính quy hoá mới là yếu tố quyết định" tự nó là một kết luận đáng viết trong báo cáo.

15 lần fit, ước lượng 50–65 phút tổng trên cấu hình đã nêu. `maxIter` cố định để giữ ngân sách thời gian.

Mỗi tổ hợp huấn luyện trên tập `train` và chấm điểm trên tập `validation`. Tổ hợp thắng được huấn luyện lại trên `train + validation` rồi mới đánh giá trên `test` ở Job 3.

**Hai tham số bắt buộc, thiếu là hỏng:**

- `coldStartStrategy="drop"` — không có nó, user hoặc phim chưa xuất hiện trong tập train sẽ cho dự đoán `NaN`, và RMSE của toàn bộ tập test trở thành `NaN`. Đây là lỗi số một khi làm ALS.
- `checkpointDir` + `checkpointInterval` — ALS lặp nhiều vòng tạo lineage RDD rất dài, dẫn tới `StackOverflowError`. Checkpoint định kỳ cắt lineage.

**Đầu ra:** mô hình tốt nhất vào `data/output/model/`, toàn bộ kết quả lưới vào `report/results/tuning.csv`.

### 5.2 Job 3 — `evaluate.py`

Chạy trên tập `test`, đúng một lần, sau khi siêu tham số đã chốt.

**Metrics dự đoán:** RMSE, MAE.

**Metrics xếp hạng @10:** Precision, Recall, NDCG, Coverage.

**Định nghĩa "liên quan" (relevant):** một phim trong tập test được tính là liên quan nếu user chấm **≥ 4.0 sao**. Ngưỡng này phải cố định và nêu rõ trong báo cáo — Precision@10 tính với ngưỡng 3.0 và với ngưỡng 4.0 cho ra hai con số khác hẳn nhau, nên con số không kèm ngưỡng là con số vô nghĩa.

**Định nghĩa Coverage:** tỷ lệ phần trăm số phim phân biệt xuất hiện trong danh sách top-10 của toàn bộ user, chia cho tổng số phim trong catalog.

**Ngưỡng tối thiểu khi sinh gợi ý: `MIN_RATINGS_FOR_RECOMMENDATION = 20`.**

Lần chạy đầu tiên không có ngưỡng này cho kết quả: ALS đạt NDCG@10 = 0.00031 trong khi baseline popularity đạt 0.03082 — thua khoảng 100 lần, dù ALS thắng cả ba baseline về RMSE. Chẩn đoán: phim mà ALS gợi ý có số lượt đánh giá **trung vị = 1** (trung bình 1.4), so với trung vị 3 của cả catalog và trung bình 268 của danh sách popularity. Factor tiềm ẩn của những phim đó được ước lượng từ đúng một quan sát, nên điểm dự đoán bị đẩy lên cực trị và chúng chiếm hết top-10 — trong khi gần như không bao giờ xuất hiện trong tập test của user.

Đây là hiện tượng đã được ghi nhận: mô hình explicit-feedback tối ưu cho sai số dự đoán **không** tối ưu cho xếp hạng top-N. Vì vậy tập ứng viên khi sinh top-N được lọc còn các phim có ít nhất 20 lượt đánh giá trong tập huấn luyện.

**Báo cáo phải nêu cả hai con số — trước lọc và sau lọc — chứ không thay thế.** Con số trước lọc là bằng chứng cho luận điểm "RMSE thấp không đồng nghĩa gợi ý tốt", vốn là kết luận có giá trị nhất của phần đánh giá. Việc lọc cũng không thiên vị ALS: baseline popularity vốn chỉ gợi ý phim có từ 268 lượt trở lên, nên ngưỡng này đưa hai bên về cùng mặt bằng thay vì ưu ái một bên.

RMSE đo sai lệch của điểm dự đoán, không đo chất lượng của danh sách gợi ý — một mô hình có RMSE tốt vẫn có thể gợi ý danh sách vô dụng. Coverage đo tỷ lệ catalog mà hệ thống thực sự gợi ý tới, phát hiện trường hợp mô hình chỉ quanh quẩn vài phim nổi tiếng.

**Ba baseline so sánh:**

1. **Global mean** — luôn dự đoán điểm trung bình toàn cục.
2. **Item mean** — dự đoán điểm trung bình của chính bộ phim đó.
3. **Popularity top-N** — gợi ý các phim được đánh giá nhiều nhất, không cá nhân hóa.

Baseline thứ ba là quan trọng nhất. Baseline popularity trong recsys mạnh một cách đáng ngạc nhiên; nhiều đồ án hoàn thành rồi mới phát hiện mô hình của mình thua nó. Có bảng so sánh này làm báo cáo đáng tin hơn hẳn.

**Đầu ra:** `report/results/metrics.csv`.

### 5.3 Job 4 — `export_recs.py`

- `recommendForAllUsers(20)` → ~3,2 triệu dòng.
- Ghi ra Parquet (lưu trữ) và SQLite có index trên `userId` (phục vụ truy vấn).
- Xuất `itemFactors` ra `item_factors.npy` + bảng ánh xạ chỉ số ↔ `movieId`, phục vụ tính "phim tương tự".

SQLite được chọn thay vì để API đọc thẳng Parquet: truy vấn theo `userId` có index cho độ trễ ổn định dưới mili giây, và thể hiện rõ ranh giới bàn giao giữa batch và serving.

---

## 6. Tầng serving

### 6.1 API (FastAPI)

Nạp lúc khởi động: kết nối SQLite chế độ chỉ đọc, `item_factors.npy` vào RAM (62.000 × rank số thực, khoảng 12–25 MB).

| Endpoint | Chức năng |
|---|---|
| `GET /api/users/{id}/recommendations?k=10` | Top-k gợi ý: tên phim, thể loại, điểm dự đoán |
| `GET /api/users/{id}/history` | Các phim user đã chấm điểm cao nhất |
| `GET /api/movies/{id}/similar?k=10` | Cosine similarity trên item factors |
| `GET /api/movies/search?q=` | Tìm phim theo tên, phục vụ ô tìm kiếm |
| `GET /api/stats` | Số liệu dataset và metrics mô hình |
| `GET /health` | Kiểm tra sống |

"Phim tương tự" tính trực tiếp bằng numpy: chuẩn hóa `item_factors`, nhân vector của phim đang xét với toàn bộ ma trận. Với 62.000 × 50 thì phép nhân này mất vài mili giây, không cần tiền tính toán toàn bộ ma trận 62.000 × 62.000.

### 6.2 Giao diện web

Một file `index.html`, JavaScript thuần, **không có bước build** (không npm, không bundler — thêm một mắt xích có thể hỏng trước giờ nộp mà không đóng góp điểm nào).

Bố cục hai khung cạnh nhau: nhập `userId` → bên trái là **lịch sử xem**, bên phải là **gợi ý**.

Đặt cạnh nhau là chủ ý: người chấm nhìn vào tự đối chiếu được "user này thích phim hành động, hệ thống gợi ý phim hành động". Sức thuyết phục trực quan này lớn hơn mọi con số RMSE trong báo cáo.

---

## 7. Kiểm thử

| Loại | Nội dung | Thời gian chạy |
|---|---|---|
| Unit | `common/metrics.py` với ví dụ nhỏ tính tay được: NDCG@3 của danh sách đã biết phải ra đúng giá trị đã biết | < 1 giây |
| Integration | Chạy trọn 4 job trên `ml-latest-small` với `local[2]`, kiểm tra các job nối được vào nhau và RMSE nằm trong ngưỡng hợp lý | ~1 phút |
| API | `TestClient` của FastAPI với SQLite fixture nhỏ | < 5 giây |

Unit test cho metrics là ưu tiên cao nhất: công thức NDCG rất dễ cài sai mà vẫn cho ra con số trông hợp lý, và sai ở đây làm hỏng toàn bộ phần đánh giá của báo cáo mà không có dấu hiệu cảnh báo.

---

## 8. Thí nghiệm khả năng mở rộng

`scripts/scaling_experiment.sh` chạy `train_als.py` với siêu tham số cố định dưới ba cấu hình:

| Cấu hình | Worker | Core mỗi worker | Tổng core |
|---|---|---|---|
| 1 | 1 | 2 | 2 |
| 2 | 2 | 2 | 4 |
| 3 | 4 | 2 | 8 |

**Lưu ý:** ba cấu hình này **ghi đè** cấu hình mặc định ở §3.1 (2 worker × 3 core). Trong thí nghiệm, số core mỗi worker giữ cố định ở 2 để biến duy nhất thay đổi là **số lượng worker** — nếu vừa đổi số worker vừa đổi core mỗi worker thì không quy kết được kết quả cho yếu tố nào. Cấu hình 2×3 core chỉ dùng cho việc chạy pipeline thường ngày.

Mỗi cấu hình chạy 3 lần lấy trung bình, vì thời gian chạy trên máy cá nhân dao động đáng kể tùy tải nền.

Ghi thời gian chạy vào `report/results/scaling.csv`, tính speedup và hiệu suất song song.

**Kỳ vọng:** speedup sẽ dưới tuyến tính. ALS phải shuffle lượng lớn dữ liệu giữa các vòng lặp, và máy chỉ có 8 core vật lý nên cấu hình 3 không còn dư tài nguyên cho hệ điều hành và driver.

Phần **giải thích tại sao speedup dưới tuyến tính** mới là nội dung có giá trị học thuật, không phải con số speedup đẹp. Báo cáo cần nêu rõ: chi phí truyền thông giữa các executor, giới hạn core vật lý, và phần tuần tự không song song hóa được (định luật Amdahl).

---

## 9. Sản phẩm bàn giao

### 9.1 Mã nguồn
Repo Git có lịch sử commit, README hướng dẫn chạy từ đầu (bao gồm bước chỉnh `.wslconfig`).

### 9.2 Biểu đồ
`report/make_figures.py` đọc các file CSV kết quả và sinh:
- Đường cong tuning: RMSE theo `rank`, mỗi `regParam` một đường.
- Đường cong speedup theo số core, kèm đường tham chiếu tuyến tính lý tưởng.
- Biểu đồ cột so sánh ALS với 3 baseline trên từng metric.

### 9.3 Ảnh chụp màn hình
Spark UI: DAG của job huấn luyện, stage timeline, executor summary.

### 9.4 Báo cáo
Cấu trúc đề xuất: đặt vấn đề → dữ liệu → kiến trúc → thuật toán ALS → thực nghiệm tuning → đánh giá và so sánh baseline → thí nghiệm mở rộng → kết luận và hạn chế.

---

## 10. Lộ trình

| Tuần | Nội dung |
|---|---|
| 1 | Docker compose chạy được; `ingest.py` và `train_als.py` thông trên `ml-latest-small`; sau đó mới scale lên `ml-25m`. |
| 2 | `evaluate.py` + baselines; `export_recs.py`; API; giao diện web. |
| 3 | Thí nghiệm mở rộng; sinh biểu đồ; chụp Spark UI; viết báo cáo. |

**Phát triển trên bộ nhỏ trước là quyết định có chủ ý.** Vòng lặp sửa lỗi trên `ml-latest-small` tính bằng giây; trên `ml-25m` tính bằng chục phút. Mọi lỗi logic nên được phát hiện ở bộ nhỏ.

---

## 11. Rủi ro

| Rủi ro | Giảm thiểu |
|---|---|
| Executor bị OOM-kill trên `ml-25m` | Chỉnh `.wslconfig` lên 12 GB; giảm `rank` tối đa xuống 50 nếu vẫn lỗi; phương án dự phòng là `ml-1m`. |
| RMSE ra `NaN` | `coldStartStrategy="drop"` — đã đưa vào thiết kế. |
| `StackOverflowError` khi ALS lặp nhiều | Checkpoint định kỳ — đã đưa vào thiết kế. |
| Executor không tìm thấy file | Mount cùng đường dẫn `/opt/data` ở mọi container — đã đưa vào thiết kế. |
| Grid search vượt ngân sách thời gian | `maxIter` cố định ở 10; có thể cắt `rank=100` nếu cần. |
| Mô hình thua baseline popularity | Đây là một kết quả hợp lệ, không phải thất bại. Báo cáo phân tích nguyên nhân (độ thưa dữ liệu, cold-start) thay vì giấu đi. |
