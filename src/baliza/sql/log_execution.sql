-- Log flow execution in meta.execution_log
-- Parameters: execution_id, flow_name, task_name, status, records_processed, bytes_processed, error_message, metadata

INSERT INTO meta.execution_log (
    execution_id,
    flow_name,
    task_name,
    start_time,
    end_time,
    status,
    records_processed,
    bytes_processed,
    error_message,
    metadata
) VALUES (
    ?,              -- execution_id
    ?,              -- flow_name
    ?,              -- task_name
    ?,              -- start_time
    ?,              -- end_time
    ?,              -- status
    ?,              -- records_processed
    ?,              -- bytes_processed
    ?,              -- error_message
    ?::JSON         -- metadata
);