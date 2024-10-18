INSERT INTO audit_log (task, affected_rows, timestamp)
SELECT 'load_raw_data', COUNT(*), CURRENT_TIMESTAMP
FROM airline_raw_data;