# Câu lệnh chạy/thoát docker-compose
docker-compose up -d
docker-compose down

# Câu lệnh chạy namenode để load file .csv vào HDFS
docker cp ./data/localfile.csv namenode:/localfile.csv \
docker cp ./data/omr.csv namenode:omr.csv \
docker cp ./data/patients.csv namenode:/patients.csv \
docker cp ./data/services.csv namenode:/services.csv \
docker cp ./data/emar.csv namenode:/emar.csv \
docker cp ./data/emar_detail.csv namenode:/emar_detail.csv \
docker exec -it namenode hdfs dfs -mkdir -p /input \
docker exec -it namenode hdfs dfs -put /localfile.csv /input/ \
docker exec -it namenode hdfs dfs -put /omr.csv /input/ \
docker exec -it namenode hdfs dfs -put /patients.csv /input/ \
docker exec -it namenode hdfs dfs -put /services.csv /input/ \
docker exec -it namenode hdfs dfs -put /emar.csv /input/ \
docker exec -it namenode hdfs dfs -put /emar_detail.csv /input/ \
docker exec -it namenode rm /omr.csv \
docker exec -it namenode rm /patients.csv \
docker exec -it namenode rm /services.csv \
docker exec -it namenode rm /emar.csv \
docker exec -it namenode rm /emar_detail.csv

# Câu lệnh nạp scala script vào spark master
docker cp ./script/sql-spark.scala spark-master:/sql-spark.scala

# Câu lệnh chạy spark-shell để chạy script
docker exec -it spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077 -i /sql-spark.scala
