CREATE TABLE IF NOT EXISTS chartevents_stream (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    subject_id VARCHAR(50),
    hadm_id VARCHAR(50),
    itemid VARCHAR(50),
    vital_name VARCHAR(100),
    value TEXT,
    valuenum DOUBLE PRECISION,
    valueuom VARCHAR(50),
    charttime TIMESTAMP,
    storetime TIMESTAMP,
    event_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chartevents_stream_timestamp ON chartevents_stream(timestamp);
CREATE INDEX IF NOT EXISTS idx_chartevents_stream_subject_id ON chartevents_stream(subject_id);
CREATE INDEX IF NOT EXISTS idx_chartevents_stream_vital_name ON chartevents_stream(vital_name);
CREATE INDEX IF NOT EXISTS idx_chartevents_stream_event_time ON chartevents_stream(event_time);