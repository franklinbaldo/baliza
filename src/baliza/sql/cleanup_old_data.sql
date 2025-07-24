-- Cleanup old data based on retention policy
-- Parameters: ${days_to_keep} for execution log, fixed 30 days for failed requests

-- Keep execution log for specified days
DELETE FROM meta.execution_log 
WHERE start_time < NOW() - INTERVAL '${days_to_keep} days';

-- Keep failed requests for 30 days (shorter retention)
DELETE FROM meta.failed_requests 
WHERE failed_at < NOW() - INTERVAL '30 days' AND resolved = true;