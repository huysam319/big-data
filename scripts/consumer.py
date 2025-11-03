import os, json, time
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mimic_chartevents_stream")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "mimic-consumer-001")  # đổi tên để đọc từ đầu
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "database")
PG_USER = os.getenv("PG_USER", "username")
PG_PASS = os.getenv("PG_PASS", "password")
PG_TABLE= os.getenv("PG_TABLE", "chartevents_stream")

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

    conn = connect_pg()
    conn.autocommit = False
    cur = conn.cursor()

    insert_sql = f"""
        INSERT INTO {PG_TABLE}
        ("timestamp","subject_id","hadm_id","itemid","vital_name","value","valuenum","valueuom","charttime","storetime","event_time")
        VALUES (%(timestamp)s,%(subject_id)s,%(hadm_id)s,%(itemid)s,%(vital_name)s,%(value)s,%(valuenum)s,%(valueuom)s,%(charttime)s,%(storetime)s,%(event_time)s)
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
        print("[consumer] Closed.")

if __name__ == "__main__":
    main()
