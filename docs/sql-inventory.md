# Inventário de SQL Inline

Este documento mapeia todo o SQL inline encontrado no código Python da aplicação.

## Arquivo: `/app/src/baliza/pncp_writer.py`

Encontradas **26** queries inline.

### Query 1

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE SCHEMA IF NOT EXISTS psa
```

### Query 2

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE TABLE IF NOT EXISTS psa.pncp_content (
                id UUID PRIMARY KEY, -- UUIDv5 based on content hash
                response_content TEXT NOT NULL,
                content_sha256 VARCHAR(64) NOT NULL UNIQUE, -- For integrity verification
                content_size_bytes INTEGER,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reference_count INTEGER DEFAULT 1 -- How many requests reference this content
            ) WITH (compression =
```

### Query 3

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE TABLE IF NOT EXISTS psa.pncp_requests (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                endpoint_url VARCHAR NOT NULL,
                endpoint_name VARCHAR NOT NULL,
                request_parameters JSON,
                response_code INTEGER NOT NULL,
                response_headers JSON,
                data_date DATE,
                run_id VARCHAR,
                total_records INTEGER,
                total_pages INTEGER,
                current_page INTEGER,
                page_size INTEGER,
                content_id UUID REFERENCES psa.pncp_content(id) -- Foreign key to content
            ) WITH (compression =
```

### Query 4

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE TABLE IF NOT EXISTS psa.pncp_raw_responses (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                endpoint_url VARCHAR NOT NULL,
                endpoint_name VARCHAR NOT NULL,
                request_parameters JSON,
                response_code INTEGER NOT NULL,
                response_content TEXT,
                response_headers JSON,
                data_date DATE,
                run_id VARCHAR,
                total_records INTEGER,
                total_pages INTEGER,
                current_page INTEGER,
                page_size INTEGER
            ) WITH (compression =
```

### Query 5

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT 1 FROM information_schema.indexes WHERE index_name = ?
```

### Query 6

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE INDEX IF NOT EXISTS {index_name}_test ON psa.pncp_raw_responses(endpoint_name)
```

### Query 7

- **Tipo**: `OTHER`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
DROP INDEX IF EXISTS {index_name}_test
```

### Query 8

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create indexes for split table architecture.
```

### Query 9

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE INDEX IF NOT EXISTS idx_requests_endpoint_date_page ON psa.pncp_requests(endpoint_name, data_date, current_page)
```

### Query 10

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE INDEX IF NOT EXISTS idx_requests_response_code ON psa.pncp_requests(response_code)
```

### Query 11

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE INDEX IF NOT EXISTS idx_requests_content_id ON psa.pncp_requests(content_id)
```

### Query 12

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE INDEX IF NOT EXISTS idx_content_hash ON psa.pncp_content(content_sha256)
```

### Query 13

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE INDEX IF NOT EXISTS idx_content_first_seen ON psa.pncp_content(first_seen_at)
```

### Query 14

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE INDEX IF NOT EXISTS idx_legacy_endpoint_date_page ON psa.pncp_raw_responses(endpoint_name, data_date, current_page)
```

### Query 15

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE INDEX IF NOT EXISTS idx_legacy_response_code ON psa.pncp_raw_responses(response_code)
```

### Query 16

- **Tipo**: `INSERT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
INSERT INTO psa.pncp_raw_responses (
                    endpoint_url, endpoint_name, request_parameters,
                    response_code, response_content, response_headers,
                    data_date, run_id, total_records, total_pages,
                    current_page, page_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

### Query 17

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT id, reference_count FROM psa.pncp_content WHERE content_sha256 = ?
```

### Query 18

- **Tipo**: `UPDATE`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
UPDATE psa.pncp_content
                    SET reference_count = reference_count + 1,
                        last_seen_at = CURRENT_TIMESTAMP
                    WHERE content_sha256 = ?
```

### Query 19

- **Tipo**: `INSERT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
INSERT INTO psa.pncp_content
                    (id, response_content, content_sha256, content_size_bytes, reference_count)
                    VALUES (?, ?, ?, ?, 1)
```

### Query 20

- **Tipo**: `INSERT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
INSERT INTO psa.pncp_requests (
                    extracted_at, endpoint_url, endpoint_name, request_parameters,
                    response_code, response_headers, data_date, run_id,
                    total_records, total_pages, current_page, page_size, content_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

### Query 21

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT content_sha256, id, reference_count FROM psa.pncp_content WHERE content_sha256 IN ({placeholders})
```

### Query 22

- **Tipo**: `INSERT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
INSERT INTO psa.pncp_content
                    (id, response_content, content_sha256, content_size_bytes, reference_count)
                    VALUES (?, ?, ?, ?, 1)
```

### Query 23

- **Tipo**: `UPDATE`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
UPDATE psa.pncp_content
                    SET reference_count = ?, last_seen_at = CURRENT_TIMESTAMP
                    WHERE content_sha256 = ?
```

### Query 24

- **Tipo**: `INSERT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
INSERT INTO psa.pncp_requests (
                    extracted_at, endpoint_url, endpoint_name, request_parameters,
                    response_code, response_headers, data_date, run_id,
                    total_records, total_pages, current_page, page_size, content_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

### Query 25

- **Tipo**: `INSERT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
INSERT INTO psa.pncp_raw_responses (
                    extracted_at, endpoint_url, endpoint_name, request_parameters,
                    response_code, response_content, response_headers, data_date, run_id,
                    total_records, total_pages, current_page, page_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

### Query 26

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT response_content FROM psa.pncp_raw_responses
            WHERE endpoint_name = ? AND data_date = ? AND current_page = ? AND response_code = 200
            LIMIT 1
```

## Arquivo: `/app/src/baliza/mcp_server.py`

Encontradas **2** queries inline.

### Query 1

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet(?)
```

### Query 2

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT * FROM contratos
```

## Arquivo: `/app/src/baliza/loader.py`

Encontradas **1** queries inline.

### Query 1

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = ?
```

## Arquivo: `/app/src/baliza/extractor.py`

Encontradas **3** queries inline.

### Query 1

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT DISTINCT
                endpoint_name,
                data_date,
                CASE
                    WHEN json_extract(request_parameters, '$.modalidade') IS NULL THEN NULL
                    ELSE CAST(json_extract(request_parameters, '$.modalidade') AS VARCHAR)
                END as modalidade
            FROM psa.pncp_raw_responses
            WHERE current_page = 1
                -- Qualquer response_code é válido (200, 404, etc.) - já foi processado
                -- Para o mês atual, ignorar dados existentes (forçar refresh)
                AND data_date < ?
```

### Query 2

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT DISTINCT
                endpoint_name,
                data_date,
                CASE
                    WHEN json_extract(request_parameters, '$.modalidade') IS NULL THEN NULL
                    ELSE CAST(json_extract(request_parameters, '$.modalidade') AS VARCHAR)
                END as modalidade,
                total_pages
            FROM psa.pncp_raw_responses
            WHERE current_page = 1
                AND total_pages > 1
                AND response_code = 200
                AND data_date >= ?
                AND data_date <= ?
                AND (
                    -- Para meses anteriores, usar dados existentes (qualquer run_id)
                    data_date < ?
                    -- Para o mês atual, apenas do run atual para forçar refresh
                    OR (data_date >= ? AND run_id = ?)
                )
```

### Query 3

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT DISTINCT
                endpoint_name,
                data_date,
                CASE
                    WHEN json_extract(request_parameters, '$.modalidade') IS NULL THEN NULL
                    ELSE CAST(json_extract(request_parameters, '$.modalidade') AS VARCHAR)
                END as modalidade,
                current_page as pagina
            FROM psa.pncp_raw_responses
            WHERE response_code = 200
                AND data_date >= ?
                AND data_date <= ?
```

## Arquivo: `/app/src/baliza/dependencies.py`

Encontradas **2** queries inline.

### Query 1

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create the default dependency container with production dependencies.
```

### Query 2

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create a test dependency container with mock implementations.
```

## Arquivo: `/app/src/baliza/archival.py`

Encontradas **5** queries inline.

### Query 1

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT DISTINCT endpoint_name, COUNT(*) as record_count
                FROM psa.pncp_raw_responses
                WHERE data_date >= ? AND data_date <= ?
                    AND response_code = 200
                GROUP BY endpoint_name
                ORDER BY endpoint_name
```

### Query 2

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT
                        extracted_at,
                        endpoint_name,
                        request_parameters,
                        response_code,
                        response_content,
                        response_headers,
                        data_date,
                        run_id,
                        total_records,
                        total_pages,
                        current_page,
                        page_size
                    FROM psa.pncp_raw_responses
                    WHERE endpoint_name = ?
                        AND data_date >= ?
                        AND data_date <= ?
                        AND response_code = 200
                    ORDER BY data_date, current_page
```

### Query 3

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_raw_responses
                    WHERE data_date >= ? AND data_date <= ?
```

### Query 4

- **Tipo**: `DELETE`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
DELETE FROM psa.pncp_raw_responses
                    WHERE data_date >= ? AND data_date <= ?
```

### Query 5

- **Tipo**: `DELETE`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
DELETE FROM psa.pncp_content WHERE reference_count = 0
```

## Arquivo: `/app/src/baliza/cli.py`

Encontradas **6** queries inline.

### Query 1

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_raw_responses
```

### Query 2

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE response_code = 200
```

### Query 3

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT endpoint_name, COUNT(*) as responses, SUM(total_records) as total_records
        FROM psa.pncp_raw_responses
        WHERE response_code = 200
        GROUP BY endpoint_name
        ORDER BY total_records DESC
```

### Query 4

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_requests
```

### Query 5

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_content
```

### Query 6

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT MIN(extracted_at), MAX(extracted_at) FROM psa.pncp_requests
```

## Arquivo: `/app/src/baliza/ui/components.py`

Encontradas **8** queries inline.

### Query 1

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create a beautiful header panel with title, subtitle, and icon.
```

### Query 2

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create a beautiful table with consistent styling.
```

### Query 3

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create a beautiful progress bar with optional features.
```

### Query 4

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create an informational panel with icon and styling.
```

### Query 5

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create a quick stats table with two columns.
```

### Query 6

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create a list of quick actions with keyboard shortcuts.
```

### Query 7

- **Tipo**: `UPDATE`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Update progress with optional new description and stats.
```

### Query 8

- **Tipo**: `UPDATE`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Update the total for the progress bar.
```

## Arquivo: `/app/src/baliza/ui/dashboard.py`

Encontradas **18** queries inline.

### Query 1

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create the main welcome header.
```

### Query 2

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create quick status overview panel.
```

### Query 3

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create quick insights panel.
```

### Query 4

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create quick actions panel.
```

### Query 5

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create storage overview panel.
```

### Query 6

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create pipeline health panel.
```

### Query 7

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create data sources health panel.
```

### Query 8

- **Tipo**: `DDL`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
Create performance metrics panel.
```

### Query 9

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_requests
```

### Query 10

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_content
```

### Query 11

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT endpoint_name, extracted_at FROM psa.pncp_requests ORDER BY extracted_at DESC LIMIT 1
```

### Query 12

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT
                        SUM(content_size_bytes) as actual_size,
                        SUM(content_size_bytes * reference_count) as theoretical_size
                    FROM psa.pncp_content
```

### Query 13

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_requests WHERE DATE(extracted_at) = CURRENT_DATE
```

### Query 14

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_requests
```

### Query 15

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_requests WHERE response_code = 200
```

### Query 16

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_content
```

### Query 17

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT COUNT(*) FROM psa.pncp_requests
```

### Query 18

- **Tipo**: `SELECT`
- **Complexidade**: `SIMPLE`
- **SQL Encontrado**:
```sql
SELECT
                        COUNT(*) as unique_content,
                        SUM(reference_count) as total_references,
                        COUNT(CASE WHEN reference_count > 1 THEN 1 END) as deduplicated
                    FROM psa.pncp_content
```
