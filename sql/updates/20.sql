ALTER TABLE dataset_upload_session_part
  ADD COLUMN IF NOT EXISTS lock_until_ms BIGINT;

ALTER TABLE dataset_upload_session_part
  DROP COLUMN IF EXISTS ws_id;

ALTER TABLE dataset_upload_session_part
  DROP COLUMN IF EXISTS valid_until_ms;
