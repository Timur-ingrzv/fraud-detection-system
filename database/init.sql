\c fraud_detections

CREATE TABLE scores (
    transaction_id TEXT PRIMARY KEY,
    score NUMERIC,
    fraud_flag INT,
    created_at TIMESTAMPTZ DEFAULT now()
);