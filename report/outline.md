# Dàn ý báo cáo — Hệ thống gợi ý phim trên MovieLens với Spark MLlib ALS

Tài liệu này là khung để bạn viết báo cáo, kèm sẵn các con số đã đo thật.
Mọi số trong đây đều lấy từ `report/results/*.csv` sau lần chạy trên `ml-25m`.

Chỗ nào ghi `[chờ]` là số liệu thí nghiệm scale chưa chạy xong.

---

## 1. Đặt vấn đề

**Nội dung cần có:**
- Bài toán gợi ý phim: dự đoán user sẽ thích phim nào dựa trên lịch sử đánh giá
- Vì sao cần xử lý phân tán: 25 triệu bản ghi, và các phép toán như nhân ma trận
  user × item không vừa bộ nhớ một tiến trình
- Phạm vi: collaborative filtering bằng ALS, không dùng nội dung phim

**Cách nói trung thực về quy mô** (quan trọng, tránh bị hỏi vặn):

> Bộ dữ liệu 25 triệu rating không vượt khả năng của một máy chủ hiện đại,
> nhưng nó đủ lớn để (a) pandas bắt đầu khó khăn, (b) đo được hiệu ứng của
> tính toán phân tán, và (c) làm lộ ra những thuật toán đúng ở quy mô nhỏ
> nhưng thất bại ở quy mô lớn — trường hợp cụ thể được trình bày ở mục 6.

---

## 2. Dữ liệu và tiền xử lý

### 2.1 Bộ dữ liệu

| | Giá trị |
|---|---|
| Bộ dữ liệu | MovieLens 25M |
| Số rating | 25.000.095 |
| Số user | 162.541 |
| Số phim trong catalog | 62.423 |
| `ratings.csv` | 678 MB |

### 2.2 Chuyển sang định dạng cột

| Chỉ số | ml-latest-small | ml-25m |
|---|---|---|
| CSV | 2,48 MB | 678 MB |
| Parquet + Snappy | 670 KB | 268 MB |
| Tỷ lệ nén | 3,7× | **2,5×** |
| Thời gian `inferSchema` | 2,29s | 14,49s |
| Thời gian schema tường minh | 0,29s | 3,00s |
| Thời gian đọc Parquet | 0,53s | **1,43s** |

**Hai phân tích đáng viết:**

**(a) Parquet chỉ thắng từ một quy mô nhất định.** Ở bộ nhỏ, đọc Parquet (0,53s)
*chậm hơn* đọc CSV (0,29s) vì chi phí metadata cố định của Parquet chưa được
khấu hao. Ở bộ lớn, Parquet (1,43s) nhanh hơn CSV (3,00s) — gấp **2,1 lần**.
Đây là kết luận có điều kiện, mạnh hơn khẳng định chung chung "Parquet nhanh hơn".

**(b) Khai báo schema tường minh tiết kiệm một lượt quét.** `inferSchema` bắt
Spark đọc toàn bộ file thêm một lượt chỉ để đoán kiểu dữ liệu. Chênh lệch đo
được: 14,49s so với 3,00s, tức **4,8 lần**. Lưu ý phương pháp: lần đo đầu tiên
của một job Spark gánh cả chi phí khởi động JVM và cấp executor, nên cần một
lượt đọc "làm nóng" trung tính trước khi bấm giờ — không có bước này, tỷ lệ đo
được ở bộ nhỏ bị thổi lên 29 lần.

### 2.3 Chiến lược phân mảnh

- Gộp về 2 file Parquet ~130 MB mỗi file
- **Không** partition theo `userId`: sẽ sinh 162.541 thư mục con, mỗi thư mục vài
  KB — lỗi *small files* kinh điển, làm chậm nghiêm trọng thao tác đọc
- Mọi job phía sau đều đọc toàn bộ dữ liệu nên partition theo cột không lọc được gì

### 2.4 Chia tập train / validation / test

Chia **theo thời gian trong từng user**: 70% rating cũ nhất → train, 15% tiếp
theo → validation, 15% mới nhất → test.

**Ba lập luận cần nêu:**

1. **Không dùng `randomSplit`** — nó cho mô hình thấy đánh giá *tương lai* của
   một user rồi bắt dự đoán đánh giá *quá khứ* của chính user đó. Đây là rò rỉ
   dữ liệu và làm điểm số đẹp giả tạo.
2. **Phải có tập validation riêng** — nếu chọn siêu tham số trên tập test rồi
   báo cáo điểm trên chính tập đó, ta đã chọn mô hình *hợp với test*, nên con số
   lạc quan hơn thực tế. Tập test chỉ được chạm **đúng một lần**.
3. **Tie-break tất định** — sắp xếp theo `(timestamp, movieId)`. MovieLens có
   nhiều user chấm hàng loạt phim trong một phiên nên trùng timestamp là chuyện
   thường; thiếu tie-break thì mỗi lần chạy cùng một rating rơi vào tập khác
   nhau và kết quả tuning mất tính tái lập.

**Ghi chú về cold-start:** số user bị loại vì có dưới 5 rating là **0** — MovieLens
đảm bảo mọi user có ít nhất 20 rating, kể cả ở `ml-25m`. Nhánh xử lý cold-start
đã được cài và kiểm thử bằng unit test, nhưng không được dữ liệu thật kích hoạt.
Nên nói rõ điều này thay vì để trống.

---

## 3. Kiến trúc hệ thống

### 3.1 Sơ đồ

```
TẦNG BATCH (Spark, hàng chục phút)      TẦNG SERVING (FastAPI, mili giây)
ingest → train → evaluate → export ──►  recs.sqlite + item_factors.npy ──► API
```

### 3.2 Hạ tầng

- Docker Compose: 1 Spark master + 2 worker (3 core / 3 GB mỗi worker)
- Mọi container mount dữ liệu tại **cùng một đường dẫn** `/opt/data` — driver và
  executor phải thấy đường dẫn giống hệt nhau, lệch là `FileNotFoundException`
- Spark 3.5.3, Python 3.8

### 3.3 Vì sao tách batch và serving

Mỗi lần khởi tạo `SparkSession` tốn 10-20 giây. Đặt việc đó trong đường xử lý
request thì mỗi lần người dùng bấm sẽ chờ 20 giây. Tầng serving chỉ đọc kết quả
mà tầng batch đã ghi ra đĩa.

**Ranh giới được bảo vệ bằng hạ tầng, không phải bằng quy ước:** image của tầng
serving là `python:3.11-slim`, **không cài Java và không cài pyspark**. Nếu ai đó
lỡ viết `import pyspark` trong `serving/`, container chết ngay lúc khởi động thay
vì âm thầm chạy chậm.

**Độ trễ đo được:** `/health` 3,9ms, "phim tương tự" 16ms.

---

## 4. Thuật toán ALS

### 4.1 Nguyên lý

ALS phân rã ma trận rating `R (user × item)` thành tích hai ma trận nhân tử
`U (user × rank)` và `V (item × rank)`. Luân phiên cố định một ma trận và giải
bình phương tối thiểu cho ma trận kia — mỗi bước là bài toán lồi có nghiệm
đóng, và các dòng độc lập nhau nên song song hoá được.

### 4.2 Hai tham số bắt buộc trong cài đặt

- `coldStartStrategy="drop"` — không có nó, user hoặc phim chưa thấy trong train
  cho dự đoán `NaN`, và `NaN` lan sang làm RMSE của **toàn bộ** tập test thành `NaN`
- `checkpointInterval=5` — ALS lặp nhiều vòng tạo chuỗi lineage RDD rất dài,
  dẫn tới `StackOverflowError`; checkpoint định kỳ cắt chuỗi đó

---

## 5. Thực nghiệm tuning

### 5.1 Lưới tìm kiếm

`rank ∈ {10, 50, 100}` × `regParam ∈ {0.01, 0.1, 0.2, 0.3, 0.5}`, `maxIter = 10`.
Tổng 15 tổ hợp, chấm trên tập **validation**.

**Vì sao `regParam` kéo tới 0.5:** lần chạy đầu với lưới `{0.01, 0.1, 0.2}` cho
`0.2` thắng — mà 0.2 là giá trị lớn nhất, tức tối ưu nằm ở **biên** và chưa kết
luận được. Mở rộng lưới để tối ưu nằm hẳn bên trong.

### 5.2 Kết quả

| rank | regParam | RMSE | fit_seconds |
|---|---|---|---|
| **10** | **0.1** | **0.81527** | **32,2** |
| 50 | 0.1 | 0.81713 | 142,4 |
| 100 | 0.1 | 0.81816 | 457,4 |
| 10 | 0.01 | 0.83260 | 65,4 |
| 10 | 0.2 | 0.85725 | 34,0 |
| 10 | 0.3 | 0.89820 | 31,4 |
| 10 | 0.5 | 0.98499 | 36,9 |

*(bảng đầy đủ 15 dòng ở `report/results/tuning.csv`, biểu đồ ở `report/figures/tuning.png`)*

### 5.3 Ba kết luận

**(a) Mô hình nhỏ nhất thắng cả về độ chính xác lẫn tốc độ.** `rank=10` cho RMSE
tốt nhất (0,81527) và nhanh hơn `rank=100` **14 lần** (32,2s so với 457,4s).
Tăng số chiều ẩn làm RMSE *tệ đi* một chút trong khi tốn gấp 14 lần thời gian.

**(b) Chính quy hoá mới là yếu tố quyết định, không phải số chiều ẩn.** Tại
`regParam=0.1`, ba rank 10/50/100 cho RMSE 0,81527 / 0,81713 / 0,81816 — chênh
0,003. Trong khi đổi `regParam` từ 0,1 sang 0,5 làm RMSE nhảy từ 0,815 lên 0,985.

**(c) Đường cong `regParam` có dạng chữ U** với đáy tại 0,1: dưới ngưỡng đó là
quá khớp, trên ngưỡng đó là dưới khớp. Tối ưu nằm bên trong lưới nên kết luận
được. Lưu ý tối ưu dịch chuyển theo quy mô: ở `ml-latest-small` đáy nằm ở 0,2.

---

## 6. Đánh giá và so sánh baseline

### 6.1 Thiết kế thí nghiệm

Chấm trên tập **test**, đúng một lần, sau khi siêu tham số đã chốt trên validation.
Mô hình thắng được huấn luyện lại trên `train + validation` trước khi chấm.

Ba baseline đối chiếu, **đều được cho cùng lượng dữ liệu `train + validation`**
mà ALS đã dùng — nếu chỉ cho `train`, baseline bị thiệt 15% dữ liệu và ALS thắng
một phần nhờ ưu thế không chính đáng:

1. Global mean — luôn dự đoán điểm trung bình toàn cục
2. Item mean — dự đoán điểm trung bình của chính phim đó
3. Popularity top-N — gợi ý phim được đánh giá **nhiều lượt nhất**, không cá nhân hoá

Metrics: RMSE, MAE cho dự đoán điểm; Precision@10, Recall@10, NDCG@10, Coverage
cho xếp hạng. Ngưỡng liên quan: rating **≥ 4.0**.

### 6.2 Kết quả

| Mô hình | RMSE | MAE | NDCG@10 | Coverage |
|---|---|---|---|---|
| `als` | **0,82578** | **0,63557** | 0,00735 | **0,03120** |
| `als_unfiltered` | 0,82578 | 0,63557 | 0,00001 | 0,02633 |
| `item_mean` | 0,96469 | 0,74265 | — | — |
| `global_mean` | 1,05997 | 0,83193 | — | — |
| `popularity` | — | — | **0,03354** | 0,00017 |

Các ô trống là **cố ý**: popularity không dự đoán điểm nên không có RMSE;
global-mean và item-mean không xếp hạng nên không có NDCG. Điền số vào cho đủ
bảng sẽ là bịa.

### 6.3 Phát hiện trung tâm — RMSE thấp không đồng nghĩa gợi ý tốt

**Đây là nội dung có giá trị nhất của báo cáo. Nên viết kỹ.**

ALS thắng rõ ràng mọi baseline về **dự đoán điểm**: RMSE 0,826 so với 0,965
(item mean) và 1,060 (global mean). Nhưng nó **thua baseline popularity 4,6 lần**
về **chất lượng xếp hạng**: NDCG@10 đạt 0,00735 so với 0,03354.

**Chẩn đoán.** Lần chạy đầu không lọc ứng viên cho NDCG@10 gần như bằng 0
(0,00001). Đo phân bố số lượt đánh giá của các phim mà ALS gợi ý:

| | Số lượt đánh giá trung bình | Trung vị |
|---|---|---|
| Phim ALS gợi ý | 1,4 | **1** |
| Toàn catalog | 10,4 | 3 |
| Phim popularity gợi ý | 268,0 | — |

ALS đề xuất những phim **gần như không ai đánh giá**. Nhân tử tiềm ẩn của chúng
được ước lượng từ đúng một quan sát, nên điểm dự đoán bị đẩy tới cực trị và
chiếm hết top-10 — trong khi chúng gần như không bao giờ nằm trong tập test của
user. Đây là hiện tượng đã được ghi nhận: **mô hình explicit-feedback tối ưu cho
sai số dự đoán thì không tối ưu cho bài toán xếp hạng top-N.**

**Biện pháp.** Lọc tập ứng viên xuống các phim có ít nhất 20 lượt đánh giá trong
`train + validation` — còn 16.358 trên 59.047 phim. NDCG@10 tăng từ 0,00001 lên
0,00735. Việc lọc **không thiên vị ALS**: baseline popularity vốn chỉ gợi ý phim
có hàng trăm lượt đánh giá, nên ngưỡng này đưa hai bên về cùng mặt bằng.

Báo cáo giữ **cả hai con số** — trước và sau lọc — vì con số trước lọc chính là
bằng chứng cho kết luận này.

**Cái giá của việc lọc.** Đo trên `ml-latest-small`: sau khi lọc, 47% user nhận
cùng một gợi ý đứng đầu, và chỉ 27% số phim đủ điều kiện từng xuất hiện trong
bất kỳ danh sách nào. Lọc làm chất lượng xếp hạng tốt lên nhưng làm gợi ý tập
trung hơn — một đánh đổi cần nêu rõ chứ không giấu.

### 6.4 Coverage — chỗ ALS thắng

Coverage của ALS là 0,0312 so với 0,00017 của popularity — **gấp 180 lần**.
Baseline popularity gợi ý đúng 10 phim cho toàn bộ 162.541 user; ALS trải rộng
trên hàng nghìn phim khác nhau. Nghĩa là ALS *có* cá nhân hoá, chỉ là việc cá
nhân hoá đó chưa chuyển thành tỷ lệ trúng cao hơn trên tập test.

---

## 7. Thí nghiệm khả năng mở rộng

### 7.1 Thiết kế

Chạy `train_als.py` với **siêu tham số cố định** (`rank=10`, `regParam=0.1` —
tổ hợp thắng) dưới ba cấu hình, mỗi cấu hình 3 lần lấy trung bình:

| Cấu hình | Worker | Core mỗi worker | Tổng core |
|---|---|---|---|
| 1 | 1 | 2 | 2 |
| 2 | 2 | 2 | 4 |
| 3 | 4 | 2 | 8 |

**Hai điểm về phương pháp:**
- Ghim siêu tham số để mỗi phép đo là **một đơn vị công việc lặp lại được**.
  Nếu chạy trọn lưới 15 tổ hợp, mỗi phép đo sẽ gộp 15 lần fit khác nhau và con
  số tăng tốc vô nghĩa.
- Giữ **cố định 2 core mỗi worker**, chỉ đổi số lượng worker — nếu đổi cả hai
  thì không quy kết được kết quả cho yếu tố nào.

### 7.2 Kết quả

| Tổng core | min | Trung bình | max | Biên độ |
|---|---|---|---|---|
| 2 (1 worker) | 168,7s | 213,3s | 273,0s | **62%** |
| 4 (2 worker) | 174,0s | 178,3s | 182,8s | 5% |
| 8 (4 worker) | 217,3s | 230,3s | 246,7s | 14% |

Speedup so với cấu hình 2 core:

| Tổng core | Theo trung bình | Theo min |
|---|---|---|
| 2 | 1,00× | 1,00× |
| 4 | 1,20× | 0,97× |
| 8 | **0,93×** | **0,78×** |

*(số liệu gốc ở `report/results/scaling.csv`, biểu đồ ở `report/figures/scaling.png`)*

### 7.3 Phân tích — phần ăn điểm

**Đây là một kết quả âm, và cần trình bày trung thực.** Thêm worker **không làm
job nhanh hơn**, và ở cấu hình 8 core còn **chậm đi rõ rệt**.

Hai cách tính cho kết luận khác nhau ở cấu hình 4 core (1,20× theo trung bình so
với 0,97× theo min) vì một lần chạy bất thường 273s ở cấu hình 2 core kéo trung
bình lên. Điểm chắc chắn ở cả hai cách: **8 core chậm hơn cả 2 core.**

**Nguyên nhân chính — thêm container không phải là thêm phần cứng.** Cụm Spark
chạy trên **một máy vật lý 8 core**. Thêm worker chỉ chia cùng 8 core đó thành
nhiều JVM hơn, mỗi JVM mang chi phí riêng (heap, GC, tiến trình executor). Ở cấu
hình 4 worker × 2 core, tổng 8 core worker cộng với driver, Spark master và hệ
điều hành **vượt quá** 8 core vật lý, nên các tiến trình tranh nhau CPU.

**Ba yếu tố cộng hưởng:**

1. **Chi phí truyền thông.** ALS shuffle dữ liệu giữa các vòng lặp — mỗi vòng,
   nhân tử user và item được phân phối lại giữa các executor. Thêm worker làm
   tăng số cặp phải trao đổi.
2. **Quá tải đăng ký (oversubscription).** 8 core worker trên 8 core vật lý
   không để lại tài nguyên cho phần còn lại của hệ thống.
3. **Định luật Amdahl.** Phần tuần tự không song song hoá được — khởi động
   SparkSession, đọc metadata, lập kế hoạch truy vấn, ghi model — đặt trần cho
   speedup bất kể thêm bao nhiêu core. Với phép đo này, phần cố định đó chiếm tỷ
   trọng đáng kể trong tổng ~170-230 giây.

**Quan sát bổ sung củng cố kết luận:** trên bộ nhỏ `ml-latest-small`, thêm worker
cũng làm job **chậm đi** (1 worker 29,6s → 2 worker 34,0s). Hiện tượng lặp lại ở
cả hai quy mô dữ liệu.

**Kết luận cho báo cáo:** mở rộng thật sự đòi hỏi **thêm máy vật lý**, không phải
thêm container trên cùng một máy. Thí nghiệm này đo được đúng giới hạn đó. Đây
là kết luận cụ thể và có bằng chứng, mạnh hơn một đường speedup đẹp mà không
giải thích được.

### 7.4 Hạn chế của phép đo — nêu chủ động

- **Chỉ 3 lần chạy mỗi cấu hình**, và cấu hình 2 core có biên độ 62% (168,7s đến
  273,0s). Với độ nhiễu đó, chênh lệch dưới ~20% giữa các cấu hình không kết
  luận được. Muốn chặt hơn cần nhiều lần chạy hơn và khoảng tin cậy.
- **Máy có tải nền** trong lúc đo (hệ điều hành, Docker Desktop, trình duyệt).
- **Phép đo bao gồm toàn bộ thời gian job**, kể cả phần khởi động cố định không
  song song hoá được. Đo riêng thời gian fit sẽ cho bức tranh khác.

---

## 8. Demo

- Giao diện web đặt **lịch sử xem** cạnh **gợi ý** để người xem tự đối chiếu
- 5 userId gợi ý sẵn với gu khác nhau rõ rệt
- Chức năng "phim tương tự" bằng cosine similarity trên nhân tử tiềm ẩn
- Độ trễ: 4-16ms, vì tầng serving không đụng tới Spark

**Nói thật về demo:** gợi ý không bám sát gu người dùng lắm — ví dụ user thích
phim hành động lại được gợi ý *Lawrence of Arabia*. Đây đúng là bộ mặt nhìn thấy
được của kết quả đã đo ở mục 6.3, và demo đang trung thực phơi bày điểm yếu của
mô hình thay vì che đi.

---

## 9. Kết luận và hạn chế

### 9.1 Kết luận

1. Xây dựng được pipeline gợi ý end-to-end trên 25 triệu rating với Spark
2. Chứng minh `rank` gần như không ảnh hưởng, chính quy hoá mới quyết định —
   và mô hình nhỏ nhất nhanh hơn 14 lần
3. Chỉ ra và chẩn đoán được hiện tượng RMSE thấp không đồng nghĩa gợi ý tốt
4. Đo được ngưỡng quy mô mà Parquet và tính toán phân tán bắt đầu có lợi

### 9.2 Hạn chế — nêu chủ động, đừng để bị hỏi

- **Cụm Spark chạy trên một máy vật lý**, không phải nhiều node thật. Không đo
  được chi phí mạng thực tế giữa các máy.
- **25 triệu rating vẫn vừa RAM một máy chủ**, chưa phải quy mô buộc phải phân tán.
- **Không dùng HDFS** — một node HDFS không chứng minh thêm điều gì so với volume mount.
- **Không có streaming** — ALS là thuật toán batch, không cập nhật online được;
  thêm streaming chỉ để trang trí.
- **Nhánh cold-start không được kích hoạt** vì MovieLens đảm bảo mọi user ≥20 rating.
- **ALS explicit-feedback không phù hợp tối ưu cho bài toán top-N.** Hướng cải
  tiến tự nhiên là dùng ALS implicit-feedback (`implicitPrefs=True`), vốn tối ưu
  trực tiếp cho xếp hạng.

---

## Phụ lục — nguồn số liệu

| Nội dung | File |
|---|---|
| Thống kê ingest | `report/results/ingest_stats.csv` |
| Kết quả tuning 15 tổ hợp | `report/results/tuning.csv` |
| Tổ hợp thắng | `report/results/best_params.csv` |
| So sánh baseline | `report/results/metrics.csv` |
| Thí nghiệm scale | `report/results/scaling.csv` |
| Biểu đồ | `report/figures/*.png` |
| Ảnh Spark UI | `report/figures/` (cần chụp tay) |
