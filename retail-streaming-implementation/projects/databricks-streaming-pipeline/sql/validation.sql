-- Change catalog explicitly for your environment.
USE CATALOG retail_dev;
SELECT validation_status, count(*) FROM dq.validated_events GROUP BY ALL;
SELECT event_id, source_file, rejection_reasons, raw_payload FROM dq.invalid_events;
SELECT * FROM dq.duplicate_audit;
SELECT * FROM dq.cdc_conflicts;
SELECT * FROM gold.customer_history ORDER BY customer_id, record_version;
SELECT * FROM gold.sales_summary ORDER BY date, hour;
SELECT * FROM gold.streaming_dashboard ORDER BY window.start DESC;
SELECT * FROM table_changes('retail_dev.silver.orders', 0);
SELECT event_type, level, timestamp, details FROM dq.pipeline_event_log
WHERE event_type IN ('flow_progress', 'update_progress') ORDER BY timestamp DESC;
-- Runtime metrics JSON is version-sensitive: retain raw details before selecting fields.
SELECT timestamp, origin.flow_name,
  details:flow_progress:metrics:num_output_rows AS output_rows,
  details:flow_progress:data_quality AS expectations
FROM dq.pipeline_event_log WHERE event_type='flow_progress';
SELECT count(*) AS errors FROM dq.pipeline_event_log WHERE level='ERROR';
