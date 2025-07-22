-- baliza: success_requests
SELECT COUNT(*) FROM psa.pncp_requests WHERE response_code = 200;
