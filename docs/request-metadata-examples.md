# Exemplos de Uso da Tabela bronze_pncp_requests

## Visão Geral

A tabela `bronze_pncp_requests` serve como registro de metadados para todas as requisições feitas à API PNCP, permitindo:

1. **Controle de duplicatas**: Evitar reprocessar os mesmos dados
2. **Monitoramento de performance**: Acompanhar success rate e parsing
3. **Debugging**: Identificar rapidamente falhas por endpoint/período
4. **Planejamento**: Estimar carga de trabalho baseado em metadados históricos

## Exemplos por Endpoint

### 1. Contratos por Data de Publicação

```sql
-- Exemplo de registro para extração mensal de contratos
INSERT INTO bronze_pncp_requests (
    endpoint_name,
    endpoint_url,
    month,
    request_parameters,
    response_code,
    total_records,
    total_pages,
    current_page,
    page_size,
    run_id,
    data_date,
    parse_status,
    records_parsed
) VALUES (
    'contratos_publicacao',
    'https://pncp.gov.br/api/consulta/v1/contratos?dataInicial=2024-07-01&dataFinal=2024-07-31&pagina=1',
    '2024-07',
    '{}', -- Sem filtros adicionais
    200,
    15420,  -- Total de contratos encontrados
    31,     -- 31 páginas de 500 registros cada
    1,      -- Primeira página
    500,
    'run_20240723_contratos',
    '2024-07-01',
    'success',
    498     -- 498 registros parseados com sucesso (2 falharam)
);
```

### 2. Contratações por Modalidade e UF

```sql
-- Exemplo com filtros específicos
INSERT INTO bronze_pncp_requests (
    endpoint_name,
    endpoint_url,
    month,
    request_parameters,
    response_code,
    total_records,
    total_pages,
    current_page,
    page_size,
    run_id,
    data_date,
    parse_status,
    records_parsed
) VALUES (
    'contratacoes_publicacao',
    'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?dataInicial=2024-07-01&dataFinal=2024-07-31&codigoModalidadeContratacao=1&uf=DF&pagina=2',
    '2024-07',
    '{"codigoModalidadeContratacao": 1, "uf": "DF"}', -- Pregão Eletrônico no DF
    200,
    1250,   -- Total de contratações encontradas com esses filtros
    25,     -- 25 páginas
    2,      -- Segunda página
    50,     -- Page size menor para contratações
    'run_20240723_contratacoes_df',
    '2024-07-01',
    'success',
    50      -- Todos os registros parseados com sucesso
);
```

### 3. Endpoint sem Range de Datas

```sql
-- Contratações com propostas abertas (sem range de datas)
INSERT INTO bronze_pncp_requests (
    endpoint_name,
    endpoint_url,
    month,              -- NULL para endpoints sem range de datas
    request_parameters,
    response_code,
    total_records,
    total_pages,
    current_page,
    page_size,
    run_id,
    data_date,          -- Data da execução, não dos dados
    parse_status,
    records_parsed
) VALUES (
    'contratacoes_proposta',
    'https://pncp.gov.br/api/consulta/v1/contratacoes/proposta?dataFinal=2024-07-23&pagina=1',
    NULL,               -- Campo NULL para endpoints sem month
    '{"dataFinal": "2024-07-23"}',
    200,
    320,    -- Contratações com propostas abertas hoje
    7,      -- 7 páginas
    1,      -- Primeira página
    50,
    'run_20240723_propostas_abertas',
    '2024-07-23',  -- Data da execução
    'success',
    50
);
```

### 4. Caso de Erro de Parsing

```sql
-- Exemplo de falha no parsing
INSERT INTO bronze_pncp_requests (
    endpoint_name,
    endpoint_url,
    month,
    request_parameters,
    response_code,
    total_records,
    total_pages,
    current_page,
    page_size,
    run_id,
    data_date,
    parse_status,
    parse_error_message,
    records_parsed
) VALUES (
    'atas_periodo',
    'https://pncp.gov.br/api/consulta/v1/atas?dataInicial=2024-07-01&dataFinal=2024-07-31&pagina=15',
    '2024-07',
    '{}',
    200,
    5680,
    12,
    15,     -- Página que falhou
    500,
    'run_20240723_atas',
    '2024-07-01',
    'failed',
    'JSON parsing error: unexpected field "novoVigenciaFim" in AtaRegistroPrecoPeriodoDTO',
    0       -- Nenhum registro parseado devido ao erro
);
```

## Queries Úteis para Monitoramento

### 1. Status de Extração por Mês/Endpoint

```sql
SELECT 
    endpoint_name,
    month,
    COUNT(*) as total_requests,
    SUM(total_records) as total_api_records,
    SUM(records_parsed) as total_parsed_records,
    ROUND(100.0 * SUM(records_parsed) / NULLIF(SUM(total_records), 0), 2) as parse_success_rate,
    COUNT(CASE WHEN parse_status = 'failed' THEN 1 END) as failed_requests
FROM bronze_pncp_requests 
WHERE month = '2024-07'
GROUP BY endpoint_name, month
ORDER BY endpoint_name;
```

### 2. Identificar Duplicatas Potenciais

```sql
-- Encontrar requisições com mesmos parâmetros que podem ser duplicatas
SELECT 
    endpoint_name,
    month,
    request_parameters,
    COUNT(*) as duplicate_count,
    STRING_AGG(run_id, ', ') as run_ids
FROM bronze_pncp_requests
GROUP BY endpoint_name, month, request_parameters
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

### 3. Performance por Endpoint

```sql
SELECT 
    endpoint_name,
    COUNT(*) as total_requests,
    AVG(total_records) as avg_records_per_request,
    AVG(total_pages) as avg_pages_per_request,
    MIN(extracted_at) as first_extraction,
    MAX(extracted_at) as last_extraction
FROM bronze_pncp_requests
WHERE extracted_at >= '2024-07-01'
GROUP BY endpoint_name
ORDER BY total_requests DESC;
```

### 4. Controle de Progresso de Extração

```sql
-- Verificar quais meses/endpoints ainda precisam ser processados
WITH expected_extractions AS (
    SELECT 
        unnest(ARRAY['contratos_publicacao', 'contratacoes_publicacao', 'atas_periodo']) as endpoint_name,
        unnest(ARRAY['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07']) as month
),
completed_extractions AS (
    SELECT DISTINCT endpoint_name, month
    FROM bronze_pncp_requests
    WHERE parse_status = 'success'
)
SELECT 
    ee.endpoint_name,
    ee.month,
    CASE WHEN ce.endpoint_name IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END as status
FROM expected_extractions ee
LEFT JOIN completed_extractions ce ON ee.endpoint_name = ce.endpoint_name AND ee.month = ce.month
ORDER BY ee.endpoint_name, ee.month;
```

## Benefícios desta Abordagem

1. **Prevenção de Duplicatas**: Query rápida para verificar se month/endpoint já foi processado
2. **Monitoramento em Tempo Real**: Acompanhar progresso e identificar falhas rapidamente
3. **Debugging Eficiente**: Logs estruturados com contexto completo
4. **Planejamento de Recursos**: Estimar tempo/recursos baseado em dados históricos
5. **Qualidade de Dados**: Tracking de success rate de parsing por endpoint

## Integração com Fase 3B

Esta tabela será essencial para:

- **Evitar reprocessamento**: Verificar se dados já foram extraídos antes de fazer novas requisições
- **Retry inteligente**: Reprocessar apenas páginas que falharam
- **Monitoramento**: Dashboard em tempo real do status de extração
- **Debugging**: Rapidamente identificar e corrigir problemas de parsing por endpoint