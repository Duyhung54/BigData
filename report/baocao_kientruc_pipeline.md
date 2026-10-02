# BÁO CÁO THIẾT KẾ KIẾN TRÚC HỆ THỐNG BIG DATA END-TO-END
## Đề tài: Hệ thống Gợi ý Phim Phân tán trên nền tảng Apache Spark & HDFS
### (Hiện trạng nghiên cứu thực nghiệm & Định hướng phát triển bổ sung)

---

## 1. KIẾN TRÚC KHÁI NIỆM TỔNG QUAN (CONCEPTUAL ARCHITECTURE)

Kiến trúc khái niệm mức cao của hệ thống được tổ chức thành **5 tầng chức năng chuẩn Enterprise**:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        1. TẦNG NGUỒN DỮ LIỆU (DATA SOURCES)                            │
│  - Tương tác lịch sử (ratings.csv)      - Danh mục phim (movies.csv)                   │
│  - Thẻ ngữ nghĩa điện ảnh (genome-*.csv) - Dòng sự kiện thời gian thực (Kafka Stream)  │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                   2. TẦNG LƯU TRỮ PHÂN TÁN (DISTRIBUTED STORAGE)                       │
│  - Apache HDFS (Data Lake Parquet Snappy, Phân đoạn khối 128 MB, Replication Factor = 3)│
│  - Redis In-Memory State Store (Lưu trữ Session Vector thời gian thực)                 │
│  - SQLite Serving Storage (Bảng gợi ý có chỉ mục B-Tree)                               │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                   3. TẦNG XỬ LÝ & TRÍ TUỆ NHÂN TẠO (PROCESSING & AI/ML)                │
│  ┌────────────────────────────────────────┐  ┌──────────────────────────────────────┐  │
│  │       NHÁNH BATCH (Apache Spark)       │  │      NHÁNH STREAM (Spark Streaming)  │  │
│  │ - Catalyst Optimizer & Tungsten Engine │  │ - Event Time Windowing qua Timestamp │  │
│  │ - Phân rã ma trận Spark MLlib ALS      │  │ - Cập nhật Session Vector tức thời   │  │
│  │ - Grid Search Tuning & Baseline Eval   │  │ - Watermarking xử lý sự kiện trễ     │  │
│  └────────────────────────────────────────┘  └──────────────────────────────────────┘  │
│  - Định hướng bổ sung: Hybrid Content-Based Filtering qua Genome Tags                  │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                     4. TẦNG DỊCH VỤ & NGHIỆP VỤ (SERVING & GATEWAY)                    │
│  - Fast Re-ranking Engine: Kết hợp danh sách gốc (Batch) và Session Vector (Redis)     │
│  - Vector Similarity Engine: Cosine Similarity trực tiếp trên RAM bằng NumPy (< 16ms)   │
│  - FastAPI Gateway: Cung cấp RESTful Endpoints bảo mật, không phụ thuộc Spark          │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                  5. TẦNG ỨNG DỤNG & NGƯỜI DÙNG (PRESENTATION & CLIENT)                 │
│  - Web UI Dashboard: Trực quan hóa song song Lịch sử xem và Gợi ý cá nhân hóa          │
│  - Khách hàng xem phim (End-Users) & Quản trị viên phân tích số liệu                   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. HIỆN TRẠNG NGHIÊN CỨU VÀ ĐỊNH HƯỚNG BỔ SUNG

Hệ thống được thiết kế giải quyết bài toán gợi ý phim cá nhân hóa quy mô lớn trên bộ dữ liệu **MovieLens 25M**:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                      PHÂN HỆ ĐÃ NGHIÊN CỨU & THỰC NGHIỆM THÀNH CÔNG                    │
│                                                                                        │
│  - Xây dựng hoàn chỉnh Pipeline Batch 4 job: Ingest ➔ Split ➔ Train ALS ➔ Export       │
│  - Tối ưu hóa Data Lake: Chuyển CSV sang Parquet (Snappy), gộp khối chuẩn 128 MB        │
│  - Huấn luyện & Tuning mô hình Spark MLlib ALS (Grid Search 15 tổ hợp)                 │
│  - Đánh giá đa chiều với 3 Baselines, chứng minh nghịch lý RMSE vs. Top-N Ranking      │
│  - Xây dựng tầng phục vụ trực tuyến độc lập (FastAPI + SQLite + In-Memory NumPy)       │
│  - Thực nghiệm đo lường khả năng mở rộng (Scaling Experiment: 1, 2, 4 Workers)        │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        CÁC PHÂN HỆ ĐỊNH HƯỚNG VÀ MONG MUỐN BỔ SUNG                     │
│                                                                                        │
│  - Mở rộng tầng lưu trữ sang cụm phân tán HDFS đa node (NameNode + DataNodes, Rep=3)   │
│  - Tích hợp Kiến trúc Lambda: Thêm Speed Layer (Apache Kafka + Spark Streaming)        │
│  - Ứng dụng cột Timestamp làm Event Replay Stream mô phỏng luồng sự kiện thời gian thực│
│  - Cập nhật Session Vector trong phiên truy cập và lưu trữ trạng thái tại Redis        │
│  - Khai thác tệp Genome Tags & Scores để triển khai Content-Based Filtering            │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. DANH MỤC DỮ LIỆU VÀ CẤU TRÚC CHI TIẾT TỪNG TỆP

### 3.1 Tổng quan định lượng bộ dữ liệu MovieLens 25M
Bộ dữ liệu nghiên cứu là **MovieLens 25M** (`ml-25m`), phát hành bởi nhóm nghiên cứu **GroupLens Research** thuộc Đại học Minnesota (Hoa Kỳ). Dữ liệu được ghi nhận trong chu kỳ gần 25 năm (từ ngày 09/01/1995 đến ngày 21/11/2019):

- **Tổng số lượt đánh giá (Ratings)**: 25.000.095 bản ghi phản hồi tường minh (Explicit Feedback).
- **Tổng số người dùng (Users)**: 162.541 người dùng hợp lệ (tiêu chí chọn lọc: mỗi người dùng đánh giá tối thiểu 20 bộ phim, mã định danh đã được ẩn danh hóa).
- **Tổng số bộ phim (Movies)**: 62.423 bộ phim có phát sinh tương tác (trong đó 59.047 phim có ít nhất một lượt đánh giá).
- **Tổng số lượt gắn thẻ nhãn (Tags)**: 1.093.360 lượt gán thẻ tự do từ người dùng.
- **Ma trận đặc trưng Tag Genome**: 15.584.448 điểm số liên quan giữa 13.816 phim và 1.128 thẻ ngữ nghĩa điện ảnh.
- **Thang đo điểm đánh giá**: Thang điểm 5 sao với bước nhảy 0.5 (10 mức: 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0). Điểm đánh giá trung bình toàn tập đạt $\mu \approx 3.53$ sao.
- **Độ thưa ma trận tương tác (Matrix Sparsity)**:
  $$\text{Sparsity} = 1 - \frac{|R|}{|U| \times |I|} = 1 - \frac{25.000.095}{162.541 \times 59.047} \approx 99.74\%$$
  Chỉ có **0.26%** tổng số ô trong không gian tương tác Người dùng - Phim mang giá trị quan sát, đặt ra bài toán xử lý ma trận thưa quy mô lớn trên nền tảng phân tán.

---

### 3.2 Bảng tổng hợp quy mô vật lý và trạng thái nghiên cứu

| Tên tệp dữ liệu | Kích thước đĩa (CSV thô) | Số lượng bản ghi (Dòng) | Dung lượng sau nén (Parquet Snappy) | Vai trò kiến trúc | Trạng thái nghiên cứu |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`ratings.csv`** | 678.26 MB | 25.000.095 | ~268 MB (Nén 2.5×, 2 blocks) | Dữ liệu tương tác cốt lõi cho ALS & Event Replay | **ĐÃ NGHIÊN CỨU & ĐANG DÙNG** |
| **`movies.csv`** | 3.04 MB | 62.423 | ~1.1 MB (1 block) | Bảng tra cứu danh mục (Dimension Lookup) | **ĐÃ NGHIÊN CỨU & ĐANG DÙNG** |
| **`tags.csv`** | 38.81 MB | 1.093.360 | Chưa nén | Phân tích từ khóa ngữ cảnh / NLP Sentiment | **ĐỊNH HƯỚNG MONG MUỐN BỔ SUNG** |
| **`genome-scores.csv`** | 435.16 MB | 15.584.448 | Chưa nén | Ma trận dày đặc đặc trưng phim cho Content-Based | **ĐỊNH HƯỚNG MONG MUỐN BỔ SUNG** |
| **`genome-tags.csv`** | 18.10 KB | 1.128 | Chưa nén | Từ điển nhãn đặc trưng điện ảnh chuẩn hóa | **ĐỊNH HƯỚNG MONG MUỐN BỔ SUNG** |
| **`links.csv`** | 1.37 MB | 62.423 | Chưa nén | Ánh xạ ID ngoài tích hợp API TMDb/IMDb Poster | **ĐỊNH HƯỚNG MONG MUỐN BỔ SUNG** |

---

### 3.3 Đặc tả chi tiết dữ liệu đã nghiên cứu và đang phục vụ hệ thống

#### 1. Tệp đánh giá tương tác: `ratings.csv`
- **Mục đích**: Chứa toàn bộ lịch sử phản hồi tường minh (Explicit Feedback) của người dùng đối với các bộ phim trong catalog.
- **Cấu trúc trường dữ liệu (Schema)**:

| Tên trường | Kiểu dữ liệu Spark | Kiểu dữ liệu SQL | Miền giá trị | Mô tả nghiệp vụ |
| :--- | :--- | :--- | :--- | :--- |
| `userId` | `IntegerType` | `INT` | $1 \le \text{ID} \le 162.541$ | Mã định danh người dùng (đã ẩn danh) |
| `movieId` | `IntegerType` | `INT` | $1 \le \text{ID} \le 209.171$ | Mã định danh bộ phim được chấm điểm |
| `rating` | `DoubleType` | `DOUBLE` | $0.5 \le \text{score} \le 5.0$ | Điểm đánh giá (bước nhảy 0.5 sao) |
| `timestamp` | `LongType` | `BIGINT` | $789.652.004 \le t \le 1.574.327.703$ | Thời điểm phát sinh sự kiện (Unix epoch seconds) |

- **Dữ liệu mẫu thực tế**:
  ```csv
  userId,movieId,rating,timestamp
  1,296,5.0,1147880044
  1,306,3.5,1147868817
  1,307,5.0,1147868828
  1,665,5.0,1147878820
  ```
- **Đặc trưng thống kê & Kỹ thuật xử lý**:
  - *Phân bố điểm*: Điểm 4.0 chiếm tỷ lệ cao nhất (~26.8%), 3.0 (~20.0%), 5.0 (~15.2%). Điểm dưới 2.5 chiếm chưa tới 15%.
  - *Hiện tượng Đuôi dài (Long-Tail)*: 20% phim phổ biến nhất tập trung hơn 80% lượt đánh giá. Để ngăn chặn mô hình ALS học quá mức (overfit) vào các phim chỉ có 1-2 lượt đánh giá, hệ thống áp dụng bộ lọc `MIN_RATINGS_FOR_RECOMMENDATION = 20`.
  - *Chia tập theo dòng thời gian (Temporal Split)*: Thay vì cắt ngẫu nhiên, hệ thống dùng hàm cửa sổ `Window.partitionBy("userId").orderBy("timestamp")` để tách 70% Train, 15% Validation, 15% Test theo đúng thứ tự thời gian của từng người dùng, ngăn ngừa triệt để rò rỉ dữ liệu tương lai.
  - *Vai trò Streaming*: Cột `timestamp` đóng vai trò Event Time làm mốc phát lại cho kiến trúc Speed Streaming (Kafka + Spark Streaming).

#### 2. Tệp danh mục phim: `movies.csv`
- **Mục đích**: Bảng tra cứu danh mục (Dimension Lookup) cung cấp thông tin tiêu đề và thể loại tương ứng với từng `movieId`.
- **Cấu trúc trường dữ liệu (Schema)**:

| Tên trường | Kiểu dữ liệu Spark | Kiểu dữ liệu SQL | Ví dụ giá trị | Mô tả nghiệp vụ |
| :--- | :--- | :--- | :--- | :--- |
| `movieId` | `IntegerType` | `INT` | `1`, `296`, `1732` | Khóa chính liên kết với bảng tương tác và bảng thẻ |
| `title` | `StringType` | `VARCHAR(255)` | `Toy Story (1995)` | Tên phát hành kèm năm công chiếu trong ngoặc đơn |
| `genres` | `StringType` | `VARCHAR(255)` | `Action\|Adventure\|Sci-Fi` | Danh sách thể loại ngăn cách bởi ký tự `\|` |

- **Dữ liệu mẫu thực tế**:
  ```csv
  movieId,title,genres
  1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
  2,Jumanji (1995),Adventure|Children|Fantasy
  3,Grumpier Old Men (1995),Comedy|Romance
  ```
- **Tập hợp 20 thể loại chuẩn hóa**:
  *Action, Adventure, Animation, Children, Comedy, Crime, Documentary, Drama, Fantasy, Film-Noir, Horror, Musical, Mystery, Romance, Sci-Fi, Thriller, War, Western, IMAX, (no genres listed)*.
- **Vai trò kỹ thuật**: Tham gia phép kết nối (Broadcast Hash Join) với bảng kết quả dự đoán của ALS để xuất ra tệp `recs.sqlite`, phục vụ truy vấn hiển thị tên phim và thể loại tức thời trên giao diện người dùng.

---

### 3.4 Đặc tả chi tiết dữ liệu định hướng cho các phân hệ mong muốn bổ sung

#### 1. Tệp gắn nhãn tự do: `tags.csv`
- **Quy mô**: 1.093.360 bản ghi, dung lượng 38.81 MB.
- **Cấu trúc trường**: `userId` (int), `movieId` (int), `tag` (string), `timestamp` (long).
- **Dữ liệu mẫu thực tế**:
  ```csv
  userId,movieId,tag,timestamp
  3,260,classic,1439472355
  3,260,sci-fi,1439472256
  4,1732,dark comedy,1573943598
  4,1732,great dialogue,1573943604
  ```
- **Định hướng kỹ thuật**:
  - Áp dụng các kỹ thuật Xử lý Ngôn ngữ Tự nhiên (NLP) như chuẩn hóa từ vựng (Lemmatization), tính trọng số TF-IDF hoặc trích xuất nhúng ngữ nghĩa (BERT/Word2Vec) để xây dựng hồ sơ cảm thụ của người dùng theo phong cách nghệ thuật, đạo diễn, và cốt truyện.

#### 2. Cặp tệp Ma trận Thẻ Ngữ nghĩa: `genome-scores.csv` & `genome-tags.csv`
- **Khái niệm Tag Genome**: Cấu trúc biểu diễn mức độ liên quan ngữ nghĩa dày đặc (Dense Matrix) giữa phim và các thuộc tính đặc trưng điện ảnh, được tính toán bằng mô hình máy học trên hàng triệu đánh giá và văn bản review từ cộng đồng MovieLens.
- **Quy mô**: 15.584.448 bản ghi điểm số liên tục giữa 13.816 bộ phim và 1.128 đặc trưng chuẩn hóa.
- **Cấu trúc trường**:
  - `genome-tags.csv` (18.10 KB): `tagId` (int: 1 -> 1.128), `tag` (string).
  - `genome-scores.csv` (435.16 MB): `movieId` (int), `tagId` (int), `relevance` (double: $0.0 \le r \le 1.0$).
- **Dữ liệu mẫu thực tế**:
  ```csv
  # genome-tags.csv:
  tagId,tag
  1,007
  19,action
  25,aliens
  260,dark comedy

  # genome-scores.csv:
  movieId,tagId,relevance
  1,1,0.02875
  1,19,0.73250
  1,25,0.04500
  ```
- **Định hướng kỹ thuật**:
  - *Content-Based Filtering*: Mỗi bộ phim $i$ được số hóa thành vector đặc trưng $v_i \in \mathbb{R}^{1128}$.
  - *Giải quyết bài toán Khởi đầu lạnh (Item Cold-Start)*: Đối với phim mới ra rạp chưa có lượt đánh giá nào trong `ratings.csv`, hệ thống tính độ tương đồng Cosine giữa vector genome của phim mới và vector quan tâm của người dùng để gợi ý ngay lập tức mà không cần chờ thu thập phản hồi cộng đồng.

#### 3. Tệp liên kết siêu dữ liệu ngoài: `links.csv`
- **Quy mô**: 62.423 bản ghi, dung lượng 1.37 MB.
- **Cấu trúc trường**: `movieId` (int), `imdbId` (string 7 ký tự), `tmdbId` (string).
- **Dữ liệu mẫu thực tế**:
  ```csv
  movieId,imdbId,tmdbId
  1,0114709,862
  2,0113497,8844
  3,0113228,15602
  ```
- **Định hướng kỹ thuật**:
  - Kết nối Gateway với REST API của **The Movie Database (TMDb)** và **IMDb**: Cho phép ứng dụng tự động truy xuất ảnh bìa chất lượng cao (Poster Image), ảnh nền (Backdrop), tóm tắt cốt truyện (Overview), trailer YouTube chính thức và thông tin đoàn làm phim (Director/Cast) để hiển thị trực tiếp trên Dashboard giao diện người dùng.

---

### 3.5 Tối ưu hóa lưu trữ phân tán và I/O (CSV vs. Parquet Snappy)

1. **Giảm áp lực I/O đĩa và băng thông mạng**:
   - `ratings.csv` dung lượng 678.26 MB được chuyển đổi sang Parquet nén Snappy còn **268 MB** (tỷ lệ nén xấp xỉ 2.5 lần).
   - Tốc độ đọc quét toàn bộ 25 triệu dòng của Parquet đạt **1.43 giây**, nhanh hơn 2.1 lần so với đọc CSV (3.00 giây).
2. **Kỹ thuật trích xuất theo cột (Column Pruning)**:
   - Trong tác vụ huấn luyện ALS, thuật toán chỉ cần 3 cột: `userId`, `movieId`, `rating`.
   - Cấu trúc hướng cột của Parquet cho phép Spark chỉ tải đúng dữ liệu của 3 cột này vào bộ nhớ, bỏ qua hoàn toàn cột `timestamp`, giảm 25% khối lượng truyền tải bộ nhớ.
3. **Quy chuẩn kích thước khối HDFS (Block Alignment)**:
   - Dữ liệu 268 MB được gom thành 2 tệp phân đoạn (~134 MB/tệp), khớp trực tiếp với kích thước khối chuẩn 128 MB của HDFS, triệt tiêu bài toán tệp nhỏ (Small Files Problem) và tối ưu hóa năng lực đọc song song của các Spark Executor.

---

## 4. THIẾT KẾ Ý TƯỞNG: SỬ DỤNG CỘT TIMESTAMP LÀM DATA STREAM (EVENT REPLAY STREAM)

### 4.1 Vấn đề thực tế
Trong môi trường nghiên cứu, hệ thống chưa có người dùng thật tương tác liên tục trên giao diện web để sinh ra dữ liệu thời gian thực.

### 4.2 Giải pháp thiết kế: Bộ mô phỏng phát lại sự kiện (Historical Event Replay Simulator)
Hệ thống tận dụng trường **`timestamp`** (thời điểm người dùng chấm điểm, lưu bằng số giây Unix) trong `ratings.csv` để làm **Event Time** cho hệ thống Streaming:

```
[ratings.csv có cột timestamp]
              │
              ▼ (Lọc tập 15% đánh giá mới nhất)
[Event Replay Producer] ──Sắp xếp tăng dần theo timestamp──┐
              │                                             │
              ▼ (Đẩy sự kiện theo tốc độ phát lại)          │
[Apache Kafka: Topic "user-interactions"]                   │
              │                                             │
              ▼ (Tiêu thụ sự kiện theo thời gian thực)      │
[Spark Structured Streaming]                                │
  - Watermarking: withWatermark("timestamp", "2 hours")     ▼
  - Cập nhật Session Vector ngắn hạn ──────────────▶ [Redis In-Memory]
                                                            │
                                                            ▼
                                                [FastAPI: Re-ranking tức thời]
```

1. **Bộ phát sự kiện (Kafka Producer Simulator)**:
   - Trích xuất 15% dữ liệu đánh giá mới nhất.
   - Sắp xếp tăng dần theo `timestamp`.
   - Một tiến trình Python định kỳ đọc các dòng này và đẩy vào Kafka topic `user-interactions` với tốc độ phát lại có thể điều chỉnh ($1\times$ theo thời gian thực hoặc tua nhanh $100\times$).
2. **Xử lý sự kiện với Spark Structured Streaming**:
   - Sử dụng cột `timestamp` làm gốc thời gian sự kiện (**Event Time**), áp dụng cơ chế đánh dấu trễ (**Watermarking**):
     `df.withWatermark("timestamp", "2 hours")` để xử lý các sự kiện đến chậm hoặc sai lệch thứ tự mạng.
   - Khi phát hiện người dùng $u$ vừa có tương tác mới tại thời điểm $t$, hệ thống lấy ngay vector đặc trưng của bộ phim đó cập nhật vào **Session Vector** trong **Redis**.
3. **Phục vụ Re-ranking trực tuyến**:
   - Khi người dùng tải trang, FastAPI lấy danh sách gợi ý nền tảng từ Batch Layer, kết hợp với Session Vector tức thời trong Redis để xếp hạng lại danh sách phim ngay lập tức.

---

## 5. THIẾT KẾ KIẾN TRÚC HẠ TẦNG PHÂN TÁN (HDFS & SPARK ON YARN)

Kiến trúc hạ tầng phân tán chi tiết của hệ thống bao gồm:

```
┌───────────────────────── HDFS CLUSTER ─────────────────────────┐
│  NameNode (Master): Quản lý Metadata, Namespace               │
│  DataNode 1 (Worker 1): Block 1, Block 2 (128 MB Parquet)      │
│  DataNode 2 (Worker 2): Block 1 (Replica), Block 3             │
└───────────────────────────────┬────────────────────────────────┘
                                │ Data Locality (Local Read)
                                ▼
┌───────────────────── SPARK ON YARN CLUSTER ────────────────────┐
│  Spark Driver (Master): Lập kế hoạch Catalyst, DAG Scheduler   │
│  Executor 1 (Node 1): Co-located DataNode 1, Fit ALS luân phiên │
│  Executor 2 (Node 2): Co-located DataNode 2, Fit ALS luân phiên │
└───────────────────────────────┬────────────────────────────────┘
                                │ Export Serving
                                ▼
┌─────────────────────── TẦNG SERVING (API) ─────────────────────┐
│  recs.sqlite (Indexed userId)  +  item_factors.npy (In-Memory)  │
│  FastAPI Gateway (Không phụ thuộc Spark/Java, Độ trễ < 16ms)   │
└────────────────────────────────────────────────────────────────┘
```

### 5.1 Hệ thống tệp phân tán HDFS (Hadoop Distributed File System)
- **Cấu trúc phân cấp Master - Worker**:
  - `NameNode`: Quản trị không gian tên phân tán, lưu trữ cây thư mục `hdfs://namenode:9000/movielens/` và bảng ánh xạ khối dữ liệu.
  - `DataNodes`: Lưu trữ các khối dữ liệu vật lý trên các nút thành viên của cụm.
- **Chiến lược phân khối 128 MB (Block Size)**:
  - Tệp `ratings.parquet` (268 MB) được chia thành 2 khối 128 MB và 1 khối phần dư. Dữ liệu không phân vùng theo `userId` nhằm loại bỏ chi phí quản lý tệp nhỏ (Small Files Problem) trên NameNode và tối ưu hóa I/O quét toàn bảng.
- **Cơ chế chịu lỗi (Replication Factor = 3)**:
  - Mỗi khối dữ liệu được nhân bản thành 3 bản sao tại các DataNode ở các tủ máy chủ khác nhau (Rack Awareness), đảm bảo an toàn dữ liệu khi một nút gặp sự cố.
- **Tối ưu hóa vị trí dữ liệu (Data Locality)**:
  - Bộ lập lịch Spark điều phối các Executor thực thi trực tiếp trên DataNode chứa khối dữ liệu tương ứng (Local Read), loại bỏ chi phí truyền tải qua mạng trong pha đọc.

### 5.2 Định dạng Parquet nén Snappy
- Giảm dung lượng từ 678 MB (CSV) xuống 268 MB (Parquet nén 2.5×).
- Lưu trữ dạng cột giúp Spark chỉ nạp 3 cột `userId`, `movieId`, `rating`, bỏ qua cột `timestamp`.
- Tốc độ đọc 25 triệu dòng của Parquet đạt 1.43 giây, nhanh gấp **2.1 lần** so với CSV (3.00 giây).

---

## 6. THIẾT KẾ TẦNG XỬ LÝ VÀ MÔ HÌNH HỌC MÁY (SPARK BATCH)

### 6.1 Chuẩn hóa trên Spark DataFrame & Tungsten Engine
- Không sử dụng RDD cấp thấp cho nghiệp vụ.
- Tận dụng **Catalyst Optimizer** để tối ưu hóa kế hoạch thực thi logic/vật lý và kích hoạt **Predicate Pushdown**.
- Quản lý bộ nhớ Off-Heap qua **Tungsten Engine**, loại bỏ chi phí dọn rác JVM Garbage Collection khi xử lý ma trận 25 triệu bản ghi.
- Khai báo Schema tường minh tĩnh qua `StructType`, tối ưu thời gian đọc tệp gấp 4.8 lần so với suy diễn tự động `inferSchema` (3.00s so với 14.49s).

### 6.2 Thuật toán Spark MLlib ALS
- Mô hình phân rã ma trận xấp xỉ hạng thấp $R \approx U \cdot V^T$.
- **Phân tách theo thời gian (Temporal Split)**: 70% Train, 15% Validation, 15% Test trên từng người dùng bằng Window function, loại bỏ hoàn toàn hiện tượng rò rỉ dữ liệu tương lai.
- **Tối ưu hóa siêu tham số (Grid Search)**: Khảo sát 15 tổ hợp tham số. Tổ hợp $rank=10, regParam=0.1, maxIter=10$ đạt hiệu quả tối ưu nhất với RMSE = 0.81527 và thời gian fit 32.2 giây.
- **Bộ lọc ứng viên đủ điều kiện (`MIN_RATINGS_FOR_RECOMMENDATION = 20`)**: Loại bỏ các bộ phim có dưới 20 lượt đánh giá, đưa chỉ số xếp hạng NDCG@10 từ 0.00001 lên 0.00735, triệt tiêu hiện tượng mô hình overfit vào phim ngách 1 đánh giá.
- **Độ bao phủ Catalog (Coverage)**: ALS đạt 0.03120 (1.842 phim), vượt trội gấp **180 lần** so với mô hình Popularity (0.00017 - chỉ quanh quẩn 10 phim).

---

## 7. THIẾT KẾ KIẾN TRÚC MỞ RỘNG: LAMBDA ARCHITECTURE

Kiến trúc mở rộng kết hợp hoàn chỉnh giữa luồng tính toán khối (Batch) và luồng xử lý dòng (Streaming):

```
                                  ┌──────────────────────────────────────────────┐
                                  │               DỮ LIỆU ĐẦU VÀO                │
                                  │  - Lịch sử 25M ratings (HDFS)                │
                                  │  - Event Replay Stream qua Timestamp (Kafka) │
                                  └──────────────────────┬───────────────────────┘
                                                         │
                      ┌──────────────────────────────────┴──────────────────────────────────┐
                      ▼                                                                     ▼
       ┌─────────────────────────────┐                                       ┌─────────────────────────────┐
       │         BATCH LAYER         │                                       │         SPEED LAYER         │
       │    (Apache Spark on HDFS)   │                                       │   (Kafka + Spark Streaming) │
       ├─────────────────────────────┤                                       ├─────────────────────────────┤
       │ - Huấn luyện ALS định kỳ    │                                       │ - Bắt sự kiện theo Timestamp│
       │ - Tối ưu toàn cục ma trận   │                                       │ - Cập nhật Session Vector   │
       │ - Xuất Top-100 phim nền tảng│                                       │ - Lưu trạng thái vào Redis  │
       └──────────────┬──────────────┘                                       └──────────────┬──────────────┘
                      │ (Batch View)                                                        │ (Real-time State)
                      ▼                                                                     ▼
       ┌─────────────────────────────┐                                       ┌─────────────────────────────┐
       │      BATCH SERVING VIEW     │                                       │      REAL-TIME STATE        │
       │     (SQLite / Parquet)      │                                       │       (Redis In-Memory)     │
       └──────────────┬──────────────┘                                       └──────────────┬──────────────┘
                      │                                                                     │
                      └──────────────────────────────────┬──────────────────────────────────┘
                                                         │
                                                         ▼
                                          ┌─────────────────────────────┐
                                          │        SERVING LAYER        │
                                          │      (FastAPI Gateway)      │
                                          ├─────────────────────────────┤
                                          │ - Re-ranking danh sách gốc  │
                                          │ - Loại trừ phim vừa xem     │
                                          │ - Phản hồi Web UI (< 20ms)  │
                                          └─────────────────────────────┘
```

1. **Batch Layer (Đã hoàn thiện)**: Chạy định kỳ trên HDFS Data Lake, giải bài toán ALS tìm đặc trưng ẩn dài hạn của người dùng và phim, sinh trước danh sách gợi ý nền tảng vào SQLite.
2. **Speed Layer (Mong muốn bổ sung)**: Sử dụng Kafka nhận luồng sự kiện phát lại từ cột `timestamp`, Spark Structured Streaming cập nhật Session Vector và lưu vào Redis.
3. **Serving Layer (Đã hoàn thiện & Sẵn sàng tích hợp)**: FastAPI truy xuất gợi ý từ SQLite kết hợp Session Vector từ Redis để Re-ranking với độ trễ phục vụ dưới 20ms.

---

## 8. VÍ DỤ MINH HỌA XUYÊN SUỐT: TỪ DỮ LIỆU ĐẾN PHỤC VỤ (AN & BÌNH)

Để làm rõ cách thức dữ liệu di chuyển qua toàn bộ kiến trúc (từ Batch đến Streaming), ta xem xét ví dụ cụ thể sau:

### Tình huống giả định
- Người dùng **An** ($u_1$): Lịch sử thích phim **Hành động**.
- Người dùng **Bình** ($u_2$): Lịch sử thích phim **Tình cảm**.
- Các phim: **Avengers** ($i_1$), **Titanic** ($i_2$), **Iron Man** ($i_3$), **The Notebook** ($i_4$).
- Không gian nhân tử ẩn: $k = 2$ (Yếu tố 1: Hành động, Yếu tố 2: Tình cảm).

---

### Giai đoạn 1: Đêm qua lúc 2h sáng (Tầng Batch xử lý)
1. **Dữ liệu trên HDFS**:
   - An đã chấm: *Avengers* (5 sao), *Iron Man* (4.5 sao).
   - Bình đã chấm: *Titanic* (5 sao).
2. **Spark Batch huấn luyện ALS trên cụm**:
   - Executor 1 giải ra vector gu dài hạn của An: $p_{\text{An}} = [0.95, 0.05]$ (thiên về hành động).
   - Executor 2 giải ra vector gu của Bình: $p_{\text{Bình}} = [0.05, 0.98]$ (thiên về tình cảm).
   - Mô hình học được vector đặc trưng các bộ phim:
     - $q_{\text{Avengers}} = [0.90, 0.10]$
     - $q_{\text{Titanic}} = [0.05, 0.95]$
     - $q_{\text{Iron Man}} = [0.92, 0.08]$
3. **Dự đoán và xuất bản sang SQLite**:
   - Điểm dự đoán An cho *Avengers*: $0.95 \times 0.90 + 0.05 \times 0.10 = 0.86$ ($\approx 5$ sao).
   - Điểm dự đoán An cho *Titanic*: $0.95 \times 0.05 + 0.05 \times 0.95 = 0.095$ ($\approx 1$ sao).
   - Danh sách gợi ý Top trong SQLite của An: **1. Avengers, 2. Iron Man**.

---

### Giai đoạn 2: Trưa nay lúc 14:00 (Tầng Speed Streaming xử lý)
- Đột nhiên trưa nay, An đổi gu và vừa xem xong bộ phim tình cảm *The Notebook* ($q = [0.08, 0.92]$), chấm 5 sao!
1. **Event Replay / Live Interaction**:
   - Cột `timestamp` ghi nhận sự kiện phát sinh lúc 14:00 $\rightarrow$ Đẩy bản ghi `{userId: 1, movieId: TheNotebook, rating: 5.0}` vào **Apache Kafka**.
2. **Spark Structured Streaming xử lý tức thời**:
   - Nhận diện sự kiện của An dựa trên Event Time.
   - Cập nhật nhanh **Session Vector** trong phiên của An bằng trung bình có trọng số:
     $$p_{\text{Session}} = 0.5 \times p_{\text{An}} + 0.5 \times q_{\text{Notebook}} = [0.515, 0.485]$$
   - Lưu $p_{\text{Session}}$ vào **Redis In-Memory** với thời gian sống TTL = 30 phút.

---

### Giai đoạn 3: Lúc 14:01 (Tầng Serving hợp nhất Re-ranking)
- An tải lại trang web (F5):
  1. **FastAPI** đọc danh sách Batch gốc trong SQLite (đang ưu tiên *Avengers*).
  2. **FastAPI** truy vấn nhanh Redis phát hiện An đang có $p_{\text{Session}}$ mới.
  3. **Thực hiện Re-ranking**:
     - Điểm số của phim *Titanic* được tính lại theo Session:
       $$0.515 \times 0.05 + 0.485 \times 0.95 = 0.486 \quad (\approx 4 \text{ sao})$$
     - Danh sách gợi ý lập tức đưa *Titanic* và các phim tình cảm khác lên đầu trang web.
  4. Toàn bộ quá trình phản hồi người dùng diễn ra trong **< 20 mili-giây** mà không cần phải chờ cụm Spark Batch chạy lại vào ban đêm.

---

## 9. BẢNG TỔNG KẾT ĐẶC TẢ VÀ ĐỊNH HƯỚNG BỔ SUNG

| Thành phần hệ thống | Phân hệ đã nghiên cứu & thử nghiệm | Định hướng mong muốn bổ sung |
| :--- | :--- | :--- |
| **Dữ liệu sử dụng** | `ratings.csv`, `movies.csv` | Tích hợp thêm `genome-scores.csv`, `genome-tags.csv`, `tags.csv`, `links.csv` |
| **Nền tảng lưu trữ** | Parquet Data Lake (chuẩn hóa tệp 128 MB) | Cụm phân tán **HDFS** đa node (NameNode + DataNodes, Rep=3) |
| **Xử lý luồng sự kiện** | Đã thiết kế kiến trúc | **Kafka + Spark Streaming** sử dụng cột `timestamp` làm Event Replay Stream |
| **Lưu trữ trạng thái dòng** | Đã thiết kế kiến trúc | **Redis In-Memory** lưu Session Vector với thời gian sống (TTL = 30 phút) |
| **Mô hình gợi ý** | Collaborative Filtering (Spark MLlib ALS) | **Hybrid RecSys**: ALS kết hợp Content-Based Filtering qua Genome Tags |
| **Tầng phục vụ API** | FastAPI + SQLite (chỉ mục B-Tree) + NumPy | FastAPI Gateway hợp nhất Batch View + Speed State (Re-ranking tức thời) |
| **Độ trễ phục vụ** | Gợi ý: < 1ms; Phim tương tự: 12-16ms | Re-ranking trực tuyến: < 20ms |
