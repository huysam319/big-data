# Câu lệnh chạy/thoát docker-compose
docker-compose up -d
docker-compose down

# Câu lệnh chạy namenode để load file .csv vào HDFS
docker cp ./data/localfile.csv namenode:/localfile.csv
docker exec -it namenode hdfs dfs -mkdir -p /input
docker exec -it namenode hdfs dfs -put /localfile.csv /input/

# Câu lệnh chạy spark-shell để chạy câu truy vấn
docker exec -it spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077

# Câu lệnh truy vấn dùng Scala:
val df = spark.read.option("header","true").csv("hdfs://namenode:8020/input/localfile.csv")
df.createOrReplaceTempView("mytable")
val result = spark.sql("SELECT category, SUM(price * quantity) AS total_sales FROM mytable GROUP BY category")
result.show()