-- baliza: insert pncp_raw_responses
INSERT INTO psa.pncp_raw_responses (
    extracted_at, endpoint_url, endpoint_name, request_parameters,
    response_code, response_content, response_headers, data_date, run_id,
    total_records, total_pages, current_page, page_size
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
