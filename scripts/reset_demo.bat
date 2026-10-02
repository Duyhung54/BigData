@echo off
echo ===================================================
echo   KHOI PHUC TRANG THAI DEMO GOC (RESET STREAMING)
echo ===================================================
echo.
echo Dang tat cac container va xoa du lieu Kafka...
docker compose -f docker/docker-compose.yml down -v

echo.
echo Xoa du lieu Delta Lake (lich su Streaming)...
if exist "data\lake" rmdir /S /Q "data\lake"
if exist "data\checkpoint" rmdir /S /Q "data\checkpoint"

echo.
echo Xoa Model va CSDL hien tai de Spark train lai tu dau...
if exist "data\output\model" rmdir /S /Q "data\output\model"
if exist "data\output\als_model" rmdir /S /Q "data\output\als_model"
if exist "data\output\recs.sqlite" del /F /Q "data\output\recs.sqlite"
if exist "data\output\.last_trained_stream_count" del /F /Q "data\output\.last_trained_stream_count"

echo.
echo Dang khoi dong lai he thong Big Data...
docker compose -f docker/docker-compose.yml up -d

echo.
echo ===================================================
echo HOAN TAT! He thong da tro ve trang thai "nguyen thuy".
echo De trinh bay truoc giang vien, ban chi can:
echo 1. Cho khoang 1-2 phut de Spark train lai model lan dau.
echo 2. Vao trang http://localhost:8000
echo ===================================================
pause
