-- baliza: insert pncp_content
INSERT INTO psa.pncp_content (
    id,
    response_content,
    content_sha256,
    content_size_bytes
)
VALUES (?, ?, ?, ?);
