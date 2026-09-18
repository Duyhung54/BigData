# MovieLens ALS RecSys

Hệ thống gợi ý phim dùng Spark MLlib ALS trên bộ dữ liệu MovieLens, chạy trên
cụm Spark Docker Compose (1 master + N worker) và phục vụ kết quả qua FastAPI.

## Yêu cầu hệ thống

- Windows 11 + WSL2, Docker Desktop (backend WSL2 bật)
- Tối thiểu 8 CPU core, 16 GB RAM trên máy host
- Cụm mặc định dùng 2 worker x 3 core x 3 GB + driver 2 GB — cần khoảng 8 GB
  RAM cấp cho WSL2 là đủ chạy, nhưng nên cấp 12 GB để có dư địa cho hệ điều
  hành và tránh OOM khi Spark shuffle dữ liệu lớn (`ml-25m`).
- `git`, PowerShell hoặc bash có sẵn `curl`/`unzip` để tải dữ liệu (hoặc chạy
  `scripts/download_data.sh` bên trong WSL/Git Bash)

## Chỉnh `%USERPROFILE%\.wslconfig`

Mặc định WSL2 tự giới hạn RAM bằng 50% RAM máy host, và Docker Desktop chạy
bên trong WSL2. Nếu không nâng giới hạn này, khi Spark chạy ALS trên
`ml-25m` với nhiều executor, hệ điều hành trong WSL2 sẽ **OOM-kill** tiến
trình worker/executor giữa chừng job — job chết mà không có exception Spark
rõ ràng, chỉ thấy container worker biến mất hoặc "Connection reset".

Sửa (hoặc tạo mới) file `%USERPROFILE%\.wslconfig`:

```ini
[wsl2]
memory=12GB
```

Sau đó áp dụng thay đổi (đóng hết terminal WSL/Docker trước):

```powershell
wsl --shutdown
```

Rồi mở lại Docker Desktop và đợi nó khởi động xong trước khi dựng cụm.

## Tải dữ liệu

```bash
./scripts/download_data.sh ml-latest-small   # bộ nhỏ để phát triển/test
./scripts/download_data.sh ml-25m            # bộ đầy đủ để chạy thật
```

Dữ liệu được tải vào `data/raw/<dataset>/` (thư mục `data/` không được commit
vào git — xem `.gitignore`).

## Dựng cụm Spark

```bash
cd docker && docker compose up -d --build && cd ..
docker compose -f docker/docker-compose.yml ps
```

Mặc định cụm có 2 worker x 3 core x 3 GB, driver 2 GB. Có thể đổi qua biến
môi trường (dùng ở Task 11 để đo scale):

```bash
SPARK_WORKER_REPLICAS=4 SPARK_WORKER_CORES=2 SPARK_WORKER_MEMORY=2g \
  docker compose -f docker/docker-compose.yml up -d --build
```

## Chạy pipeline

Các job Spark nằm dưới `src/jobs/` (sẽ được thêm dần ở các task sau). Chạy
một job bất kỳ bằng `spark-submit` bên trong container `spark-master`, ví dụ:

```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  src/jobs/<ten_job>.py
```

## Chạy test

```bash
./scripts/test.sh
```

Script này chạy `pytest` bên trong container `spark-master` (cần cụm đã
`up`). Kỳ vọng: toàn bộ test PASS.

> Nếu chạy từ Git Bash trên Windows (không phải WSL), MSYS có thể tự đổi
> đường dẫn `/opt/app/tests` thành đường dẫn Windows và khiến `pytest` báo
> "file or directory not found". Nếu gặp lỗi này, chạy
> `MSYS_NO_PATHCONV=1 ./scripts/test.sh` hoặc chạy script từ trong WSL.

## URL

- Spark Master UI: http://localhost:8080 (kỳ vọng thấy 2 worker ở trạng thái ALIVE)
- Spark Driver UI (khi có job đang chạy qua `spark-submit`): http://localhost:4040
- API phục vụ gợi ý (từ Task 9-10 trở đi): http://localhost:8000
