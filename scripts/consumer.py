import os, json, time
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch

# Choose engine type: rule-based or model-based
USE_TRAINED_MODELS = os.getenv("USE_TRAINED_MODELS", "false").lower() == "true"

if USE_TRAINED_MODELS:
    # Use actual trained Spark models (requires Spark)
    from pyspark.sql import SparkSession
    from drug_recommendation_engine_with_models import DrugRecommendationEngineWithModels
    print("[consumer] Using trained Spark ML models (requires Spark session)")
else:
    # Use rule-based engine (lightweight, no Spark)
    from drug_recommendation_engine import DrugRecommendationEngine
    print("[consumer] Using rule-based engine (lightweight)")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mimic_chartevents_stream")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "mimic-consumer-001")
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "database")
PG_USER = os.getenv("PG_USER", "username")
PG_PASS = os.getenv("PG_PASS", "password")
PG_TABLE= os.getenv("PG_TABLE", "chartevents_stream")
PG_RECO_TABLE = os.getenv("PG_RECO_TABLE", "abnormal_events")

def connect_pg():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

def main():
    bootstrap = [h.strip() for h in BOOTSTRAP.split(",") if h.strip()]
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap,
        group_id=GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        max_poll_records=100,
        session_timeout_ms=30000,
        request_timeout_ms=40000,
    )

    print(f"[consumer] Listening on topic '{TOPIC}' → writing to table '{PG_TABLE}'")
    print(f"[consumer] Drug recommendation engine enabled - will detect abnormalities and recommend drugs")

    # Initialize drug recommendation engine
    if USE_TRAINED_MODELS:
        # Initialize Spark for model inference
        spark = SparkSession.builder \
            .appName("DrugRecommendationInference") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

        # Resolve model directory:
        # - Default to the rolling "latest" symlink created by the notebook
        # - If a latest.txt file is provided, read the actual path
        model_dir_env = os.getenv("MODEL_DIR", "models/drug_recommendation/latest")
        if model_dir_env.endswith(".txt") and os.path.exists(model_dir_env):
            try:
                with open(model_dir_env, "r") as _f:
                    model_dir_env = _f.read().strip()
            except Exception:
                pass

        MODEL_DIR = model_dir_env

        recommendation_engine = DrugRecommendationEngineWithModels(
            spark=spark,
            model_dir=MODEL_DIR
        )
        print(f"[consumer] Loaded trained models from {MODEL_DIR}")
    else:
        recommendation_engine = DrugRecommendationEngine()
        print("[consumer] Using rule-based recommendations")

    conn = connect_pg()
    conn.autocommit = False
    cur = conn.cursor()

    insert_sql = f"""
        INSERT INTO {PG_TABLE}
        ("timestamp","subject_id","hadm_id","itemid","vital_name","value","valuenum","valueuom","charttime","storetime","event_time")
        VALUES (%(timestamp)s,%(subject_id)s,%(hadm_id)s,%(itemid)s,%(vital_name)s,%(value)s,%(valuenum)s,%(valueuom)s,%(charttime)s,%(storetime)s,%(event_time)s)
    """
    # Immediate insert for AI recommendations (abnormal events)
    insert_reco_sql = f"""
        INSERT INTO {PG_RECO_TABLE}
        (subject_id, hadm_id, abnormal_vital, abnormal_value, all_vital_signs, abnormalities_count, recommendations, event_time, charttime, created_at)
        VALUES (%(subject_id)s, %(hadm_id)s, %(abnormal_vital)s, %(abnormal_value)s, %(all_vital_signs)s::jsonb, %(abnormalities_count)s, %(recommendations)s::jsonb, %(event_time)s, %(charttime)s, NOW())
    """

    try:
        batch = []
        last_flush = time.time()

        while True:
            records = consumer.poll(timeout_ms=500)  # chờ 1s/lần
            if not records:
                # flush theo nhịp thời gian
                if batch and time.time() - last_flush > 2:
                    execute_batch(cur, insert_sql, batch, page_size=500)
                    conn.commit()
                    batch.clear()
                    last_flush = time.time()
                continue

            for tp, msgs in records.items():
                for m in msgs:
                    j = m.value
                    print(f"[{tp.topic} p{tp.partition} @offset {m.offset}] {json.dumps(j, ensure_ascii=False)}")
                    
                    # Process message through drug recommendation engine
                    recommendation_result = recommendation_engine.process_kafka_message(j)
                    
                    # If abnormality detected, output recommendations
                    if recommendation_result:
                        print("\n" + recommendation_engine.format_recommendation_output(recommendation_result))
                        # Write recommendation immediately to PostgreSQL (no batching)
                        try:
                            reco_row = {
                                "subject_id": recommendation_result.get("subject_id"),
                                "hadm_id": recommendation_result.get("hadm_id"),
                                "abnormal_vital": recommendation_result.get("abnormal_vital"),
                                "abnormal_value": float(recommendation_result.get("abnormal_value")) if recommendation_result.get("abnormal_value") is not None else None,
                                "all_vital_signs": json.dumps(recommendation_result.get("all_vital_signs", {}), ensure_ascii=False),
                                "abnormalities_count": int(recommendation_result.get("abnormalities_count", 0)),
                                "recommendations": json.dumps(recommendation_result.get("recommendations", []), ensure_ascii=False),
                                "event_time": recommendation_result.get("event_time"),
                                "charttime": recommendation_result.get("charttime"),
                            }
                            cur.execute(insert_reco_sql, reco_row)
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            print(f"[consumer] ERROR writing recommendation: {e}")
                        
                        # Optionally save recommendations to a separate table or file
                        # You can extend this to write recommendations to PostgreSQL
                    
                    # Prepare row for PostgreSQL insert
                    row = {
                        "timestamp":  j.get("event_time"),   # hoặc bạn dùng m.timestamp nếu muốn
                        "subject_id": j.get("subject_id"),
                        "hadm_id":    j.get("hadm_id"),
                        "itemid":     int(j["itemid"]) if j.get("itemid") is not None else None,
                        "vital_name": j.get("vital_name"),
                        "value":      j.get("value"),
                        "valuenum":   float(j["valuenum"]) if j.get("valuenum") not in (None, "") else None,
                        "valueuom":   j.get("valueuom"),
                        "charttime":  j.get("charttime"),
                        "storetime":  j.get("storetime"),
                        "event_time": j.get("event_time"),
                    }
                    batch.append(row)

    except KeyboardInterrupt:
        print("\n[consumer] Stopping...")
    except Exception as e:
        conn.rollback()
        print(f"[consumer] ERROR: {e}")
        raise
    finally:
        try:
            if batch:
                execute_batch(cur, insert_sql, batch, page_size=500)
                conn.commit()
        except Exception:
            conn.rollback()
        cur.close()
        conn.close()
        consumer.close()
        if USE_TRAINED_MODELS and 'spark' in locals():
            spark.stop()
        print("[consumer] Closed.")

if __name__ == "__main__":
    main()
