-- Cleanup old data based on retention policy
-- Parameters: ${days_to_keep} for execution log, fixed 30 days for failed requests

-- Keep execution log for specified days
DELETE FROM meta.execution_log 
WHERE start_time < NOW() - INTERVAL '${days_to_keep} days';

-- TODO: The retention period for failed requests is hardcoded to 30 days.
-- Keep failed requests for specified days
DELETE FROM meta.failed_requests 
WHERE failed_at < NOW() - INTERVAL '${failed_requests_days_to_keep} days' AND resolved = true;