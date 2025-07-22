-- baliza: insert pncp_requests
INSERT INTO psa.pncp_requests (
    extracted_at, endpoint_url, endpoint_name, request_parameters,
    response_code, response_headers, data_date, run_id,
    total_records, total_pages, current_page, page_size, content_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
