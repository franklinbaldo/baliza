-- baliza: recent_contracts
SELECT COUNT(*) FROM psa.pncp_requests WHERE DATE(extracted_at) = CURRENT_DATE;
