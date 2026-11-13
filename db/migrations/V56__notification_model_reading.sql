-- The model's accepted reading of wording the fixed rules do not recognise. Later steps of the
-- event (source answer, mapping confirmation, recording) reuse it instead of asking the model again.
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS model_reading JSONB;
