@echo off
echo ===================================================
echo   KHOI PHUC TRANG THAI DEMO GOC (RESET STREAMING)
echo ===================================================
echo.
echo Dang tat cac container va xoa du lieu Kafka + HDFS...
rem down -v xoa ca volume cua Kafka va HDFS (namenode/datanode) -> HDFS trong rong.
docker compose -f docker/docker-compose.yml down -v

echo.
echo Xoa du lieu Delta Lake (lich su Streaming) o o local...
rem CHI xoa ratings_delta. Truoc day xoa ca data\lake, mat luon ratings.parquet va
rem movies.parquet (du lieu lich su, vd. ml-25m) - khong lien quan toi streaming.
if exist "data\lake\ratings_delta" rmdir /S /Q "data\lake\ratings_delta"
if exist "data\checkpoint" rmdir /S /Q "data\checkpoint"

echo.
echo Xoa Model va CSDL hien tai de Spark train lai tu dau...
if exist "data\output\model" rmdir /S /Q "data\output\model"
if exist "data\output\als_model" rmdir /S /Q "data\output\als_model"
if exist "data\output\recs.sqlite" del /F /Q "data\output\recs.sqlite"
if exist "data\output\.last_trained_stream_count" del /F /Q "data\output\.last_trained_stream_count"

echo.
echo Dang khoi dong lai he thong Big Data (kem HDFS theo docker\.env)...
docker compose -f docker/docker-compose.yml up -d

echo.
echo Cho 30 giay de NameNode va 2 DataNode dang ky xong...
timeout /t 30 /nobreak >nul
rem Neu dang chay che do khong HDFS thi container namenode khong ton tai: bo qua.
docker exec namenode hdfs dfsadmin -safemode wait 2>nul

echo.
echo Nap lai du lieu lich su (CSV -^> Parquet)...
rem Bat buoc o che do HDFS: down -v vua xoa sach HDFS, khong co buoc nay thi
rem train-als-stream khong con du lieu de train va web trong tron.
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/src/jobs/ingest.py

echo.
echo ===================================================
echo HOAN TAT! He thong da tro ve trang thai "nguyen thuy".
echo De trinh bay truoc giang vien, ban chi can:
echo 1. Cho vai phut de Spark train lai model lan dau.
echo 2. Vao trang http://localhost:8000
echo ===================================================
pause
