-- Create table to store AI recommendation outputs from consumer.py
-- Uses JSONB for vital signs and recommendation details
CREATE TABLE IF NOT EXISTS abnormal_events (
    id                   BIGSERIAL PRIMARY KEY,
    subject_id           BIGINT,
    hadm_id              BIGINT,
    abnormal_vital       TEXT,
    abnormal_value       DOUBLE PRECISION,
    all_vital_signs      JSONB NOT NULL DEFAULT '{}'::jsonb,
    abnormalities_count  INT NOT NULL DEFAULT 0,
    recommendations      JSONB NOT NULL DEFAULT '[]'::jsonb,
    event_time           TIMESTAMPTZ,
    charttime            TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Helpful indexes for querying recent/active alerts
CREATE INDEX IF NOT EXISTS idx_ai_reco_event_time ON abnormal_events (event_time);
CREATE INDEX IF NOT EXISTS idx_ai_reco_created_at ON abnormal_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_reco_vital ON abnormal_events (abnormal_vital);
CREATE INDEX IF NOT EXISTS idx_ai_reco_subject ON abnormal_events (subject_id, hadm_id);

