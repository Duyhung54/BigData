# Báo cáo: Tích hợp HDFS vào Hệ thống Streaming Big Data

Việc bổ sung HDFS (Hadoop Distributed File System) là mảnh ghép cuối cùng để biến dự án này thành một hệ thống Big Data phân tán đúng nghĩa. Trong các hệ thống cũ, dữ liệu được lưu trên ổ cứng cục bộ (Local File System). Việc chuyển sang HDFS giúp hệ thống có khả năng lưu trữ không giới hạn (Scale-out) và chống mất mát dữ liệu.

---

## 1. Kiến trúc Cluster HDFS và Phân bổ Node

Đối với dự án này (chạy trên môi trường giả lập Docker), kiến trúc HDFS được phân bổ như sau:

### A. Số lượng NameNode: 1 Node
* **Định nghĩa:** NameNode là "Não bộ" của HDFS. Nó không chứa dữ liệu thực tế mà chỉ chứa Metadata (Sổ cái ghi chép xem file A đang nằm ở đâu, chia làm mấy cục block).
* **Tại sao lại là 1?** Trong môi trường Demo/Local, chúng ta chỉ cần **1 Active NameNode** để tiết kiệm RAM. 
* *(Lưu ý khi báo cáo giảng viên):* Trong môi trường Production thực tế, doanh nghiệp LUÔN LUÔN dùng **2 NameNodes (1 Active, 1 Standby)** kết hợp với Zookeeper để đảm bảo High Availability (HA). Nếu NameNode chính bị cháy ổ cứng, node phụ sẽ lên thay ngay lập tức để hệ thống không bị "chết đứng" (Single Point of Failure).

### B. Số lượng DataNode: 2 Nodes (hoặc 3 Nodes)
* **Định nghĩa:** DataNode là "Kho bãi". Chứa các block dữ liệu thực tế được chia nhỏ.
* **Tại sao lại là 2 (hoặc 3) thay vì 1?**
  1. **Tính chịu lỗi (Fault Tolerance):** HDFS có cơ chế Nhân bản (Replication). Nếu cấu hình Replication = 2, mỗi mẩu dữ liệu của bạn sẽ được copy làm 2 bản để ở 2 DataNode khác nhau. Rút điện DataNode 1, dữ liệu vẫn còn ở DataNode 2.
  2. **Xử lý song song (Data Locality):** Khi Spark (gồm 2 Spark Workers) đọc dữ liệu, nó sẽ ưu tiên "Worker nào ở gần DataNode nào thì đọc dữ liệu ở đó". Điều này giúp tăng tốc độ đọc dữ liệu lên gấp đôi.

---

## 2. Quy trình xử lý toàn hệ thống (Project Processes) sau khi có HDFS

Khi HDFS được đưa vào, toàn bộ luồng dữ liệu (Data Pipeline) sẽ thay đổi không gian lưu trữ. Dưới đây là bức tranh toàn cảnh các tiến trình (Processes):

### Tiến trình 1: Thu thập Dữ liệu (Ingestion - Batch & Stream)
* **Kafka & FastAPI (Producer):** Giao diện Web gửi tin nhắn vào Kafka (giữ nguyên).
* **Spark Streaming (Consumer):** Tiến trình `stream_ingest.py` đọc tin nhắn từ Kafka. Thay vì ghi vào ổ đĩa C:/D:/E:, nó sẽ gửi lệnh tới NameNode để xin cấp phát vùng nhớ, sau đó ghi trực tiếp dữ liệu vào **DataNode 1 và DataNode 2** dưới dạng Delta Lake (`hdfs://namenode:9000/data/lake/ratings_delta`).

### Tiến trình 2: Xử lý Máy học (Machine Learning - Spark ALS)
* **Đọc phân tán:** Tiến trình `train_als_stream.py` yêu cầu đọc dữ liệu. NameNode báo vị trí, các **Spark Workers** sẽ song song tải các khối Parquet và Delta từ các **DataNodes** lên thẳng RAM của mình.
* **Train phân tán:** Thuật toán ALS chia ma trận khổng lồ ra tính toán trên nhiều Spark Worker.
* **Lưu Model:** Model sau khi train xong lại được xuất ngược về HDFS (`hdfs://namenode:9000/data/output/als_model`) để an toàn và chia sẻ cho các job khác.

### Tiến trình 3: Chuyển đổi và Phục vụ (Serving)
* **HDFS -> SQLite:** Tiến trình `export_recs.py` sẽ kéo model từ HDFS về, tính toán ra danh sách Top 10 phim gợi ý. 
* **Lưu cục bộ:** Kết quả gợi ý (vốn rất nhỏ, chỉ vài chục MB) sẽ được ghi vào file `recs.sqlite` trên ổ cứng cục bộ của server. 
* *Lý do:* HDFS sinh ra để lưu file siêu to (hàng Terabyte), nhưng tốc độ đọc ngẫu nhiên (Random Read) cực chậm. FastAPI cần phản hồi cho user trên Web dưới 100ms, nên nó phải đọc từ SQLite (nằm ở Local/RAM) chứ tuyệt đối không đọc trực tiếp từ HDFS.

---

## 3. Cách bổ sung HDFS vào dự án (Cấu hình Docker Compose)

Nếu bạn muốn tích hợp thật vào dự án, bạn sẽ cần thêm cụm container sau vào file `docker-compose.yml`:

```yaml
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    ports:
      - 9870:9870
      - 9000:9000
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./hadoop.env

  datanode1:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode1
    depends_on:
      - namenode
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - ./hadoop.env

  datanode2:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode2
    depends_on:
      - namenode
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - ./hadoop.env
```
*(Lưu ý: HDFS tiêu tốn khá nhiều RAM (khoảng 2-3GB cho cụm này). Khi đưa vào, bạn phải sửa file `src/config.py` để đổi toàn bộ đường dẫn `/opt/data/` thành `hdfs://namenode:9000/data/`).*
