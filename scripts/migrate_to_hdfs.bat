@echo off
echo ==========================================================
echo        CONG CU TU DONG NANG CAP HE THONG LEN HDFS
echo ==========================================================
echo.
echo [1/5] Dang tat he thong Local cu...
docker compose -f docker\docker-compose.yml down -v

echo.
echo [2/5] Dang khoi dong he thong moi voi Cum HDFS (1 NameNode + 2 DataNode)...
docker compose -f docker\docker-compose-hdfs.yml up -d

echo.
echo [3/5] Dang cho 20 giay de NameNode va DataNode khoi dong hoan toan...
timeout /t 20 /nobreak >nul

echo.
echo [4/5] Tao cac thu muc tren HDFS va cap quyen...
docker exec namenode hdfs dfs -mkdir -p /opt/data/lake
docker exec namenode hdfs dfs -mkdir -p /opt/data/output
docker exec namenode hdfs dfs -mkdir -p /opt/data/checkpoint
docker exec namenode hdfs dfs -chmod -R 777 /opt/data

echo.
echo [5/5] Dang chuyen doi file CSV thô thanh Parquet va day len HDFS...
echo (Tien trinh nay co the mat 1-2 phut tuy toc do may tinh)
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/src/jobs/ingest.py

echo.
echo ==========================================================
echo CHUC MUNG! HE THONG CUA BAN DA DUOC NANG CAP LEN HDFS THU TIEP.
echo - Web API van truy cap o http://localhost:8000
echo - Spark Master van truy cap o http://localhost:8080
echo - De quan ly file tren HDFS, truy cap http://localhost:9870
echo.
echo De tat he thong HDFS sau nay, hay dung lenh:
echo docker compose -f docker\docker-compose-hdfs.yml down
echo ==========================================================
pause
