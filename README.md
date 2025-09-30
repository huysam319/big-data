docker-compose up -d
docker-compose down

docker cp ./data/localfile.csv namenode:/localfile.csv
docker exec -it namenode hdfs dfs -put /localfile.csv /input/

docker exec -it spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077

Scala:
val df = spark.read.option("header","true").csv("hdfs://namenode:8020/input/localfile.csv")
df.createOrReplaceTempView("mytable")
val result = spark.sql("SELECT category, SUM(price * quantity) AS total_sales FROM mytable GROUP BY category")
result.show()