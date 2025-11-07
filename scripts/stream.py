# mim ic_chartevents_stream.py
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, to_timestamp, struct, to_json, lit,
    monotonically_increasing_id, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
import os

# =========================
# Config — CHỈNH SỬA PHẦN NÀY
# =========================
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'

HDFS_PATH = os.getenv("HDFS_PATH", "hdfs://namenode:8020") 
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mimic_chartevents_stream")
ROWS_PER_SECOND = int(os.getenv("ROWS_PER_SECOND", "2"))
SUBJECT_ID = 10017531

# =========================
# Spark Session
# =========================
spark = (
    SparkSession.builder
    .appName("MIMIC-IV Chartevents")
    .config("spark.sql.session.timeZone", "UTC")   # đồng bộ thời gian
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.shuffle.partitions", "32")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================
# 1) Nạp dữ liệu nguồn
# =========================
df_all = spark.read.parquet(HDFS_PATH + "/data/chartevents.parquet")
src_df = df_all.filter(col("subject_id") == SUBJECT_ID)

# =========================
# 2) Chuẩn hóa cột thời gian & lọc/keep cột chính
# =========================
# Nhiều build MIMIC có charttime/storetime dạng timestamp hoặc string. Ta ép kiểu an toàn.
src_df = (
    src_df
    .withColumn("charttime_ts", to_timestamp(col("charttime")))
    .withColumn("storetime_ts", to_timestamp(col("storetime")))
)

# Event time: ưu tiên charttime, nếu thiếu dùng storetime
src_df = src_df.withColumn("event_time", coalesce(col("charttime_ts"), col("storetime_ts")))

# Giữ các cột tiêu biểu
keep_cols = [
    "subject_id", "hadm_id", "icustay_id",
    "itemid", "value", "valuenum", "valueuom",
    "charttime_ts", "storetime_ts", "event_time"
]
src_df = src_df.select(*[c for c in keep_cols if c in src_df.columns])

# Bỏ null event_time (không phát được theo thời gian)
src_df = src_df.filter(col("event_time").isNotNull())

# =========================
# 3) Ánh xạ itemid → vital_name
# =========================
# Lọc ra các itemid có liên quan đến các thiết bị trong ICU
icu_device_itemids = [
    220050, 220051,        # Blood Pressure
    220045,                # Heart Rate
    220210,                # Respiratory Rate
    220277,                # Oxygen Saturation
    223762,                # Temperature
]

icu_device_itemids = [str(i) for i in icu_device_itemids]

# Lọc dữ liệu chỉ lấy các itemid thuộc danh sách các thiết bị trong ICU
src_df = src_df.filter(col("itemid").cast("string").isin(icu_device_itemids))

mapping_rows = [
    # Blood Pressure
    ("220050", "bp_systolic"),
    ("220051", "bp_diastolic"), 
    
    # Heart Rate
    ("220045", "heart_rate"),
    
    # Respiratory Rate
    ("220210", "respiratory_rate"),
    
    # Oxygen Saturation
    ("220277", "spo2"),
    
    # Temperature
    ("223762", "temperature_celsius"),
]
schema = StructType([
    StructField("itemid", StringType(), False),
    StructField("vital_name", StringType(), False),
])
map_df = spark.createDataFrame(mapping_rows, schema)

# itemid trong nguồn có thể là int; ép về string để join ổn định
src_df = src_df.withColumn("itemid_str", col("itemid").cast("string"))
fact_df = (
    src_df.join(map_df, src_df.itemid_str == map_df.itemid, "left")
          .drop(map_df.itemid)
)

# =========================
# 4) Sắp theo thời gian và tạo chỉ số tuần tự
# =========================
w = Window.partitionBy(lit(1)).orderBy(col("event_time").asc(), col("itemid_str").asc())
ordered_df = fact_df.withColumn("seq", row_number().over(w) - 1)  # bắt đầu từ 0

# =========================
# 5) Tạo nguồn stream và join để phát lần lượt
# =========================
# rate source sinh các giá trị (timestamp, value) tăng dần theo ROWS_PER_SECOND.
total_records = ordered_df.count()
if total_records == 0:
    raise ValueError("No records for SUBJECT_ID — nothing to stream.")

rate_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", ROWS_PER_SECOND)
    .load()
    .withColumnRenamed("value", "tick")
    .withColumnRenamed("timestamp", "tick_ts")
)

stream_df = (
    rate_df
    .withColumn("seq", col("tick") % total_records)
    .join(ordered_df.hint("broadcast"), "seq")
    .drop("tick", "tick_ts")
)

# Chuẩn hóa payload JSON cho Kafka
payload_df = (
    stream_df.select(
        col("event_time").alias("timestamp"),
        to_json(struct(
            col("subject_id"),
            col("hadm_id"),
            col("itemid_str").alias("itemid"),
            col("vital_name"),
            col("value"),
            col("valuenum"),
            col("valueuom"),
            col("charttime_ts").alias("charttime"),
            col("storetime_ts").alias("storetime"),
            col("event_time"),
        )).alias("json_value")
    )
)

# =========================
# 6) Ghi ra Kafka
# =========================
kafka_stream = (
    payload_df
    .select(
        col("json_value").cast("string").alias("value"),
        lit(None).cast("string").alias("key")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", KAFKA_TOPIC)
    .option("checkpointLocation", os.getenv("KAFKA_CHECKPOINT", "/tmp/spark_checkpoints/mimic_kafka"))
    .outputMode("append")
    .start()
)

# =========================
# 7) Chờ các stream
# =========================
spark.streams.awaitAnyTermination()
