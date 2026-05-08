-- NOTE: Replace <catalog> with your workshop catalog name before running.
CREATE OR REFRESH STREAMING TABLE booking_fraud_flags
COMMENT 'Fraud markers for bookings, ingested via Auto Loader from the shared landing volume'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
    booking_id,
    flag,
    reason,
    CAST(flagged_at AS TIMESTAMP)    AS flagged_at,
    confidence,
    _metadata.file_path              AS source_file,
    _metadata.file_modification_time AS source_file_ts
FROM STREAM read_files(
    '/Volumes/<catalog>/shared/landing/booking_fraud_flags/',
    format => 'json'
);
