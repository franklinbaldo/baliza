# Estratégia de Extração PNCP - Mapeamento por Fases

## Fase 2A (MVP) - 3 Endpoints Essenciais

### **Objetivo:** Pipeline básico funcional com dados principais

```python
PHASE_2A_ENDPOINTS = {
    "contratacoes_publicacao": {
        "path": "/v1/contratacoes/publicacao",
        "method": "GET",
        "priority": 1,
        "schedule": "daily",
        "required_params": ["dataInicial", "dataFinal", "codigoModalidadeContratacao", "pagina"],
        "optional_params": ["uf", "cnpj", "codigoUnidadeAdministrativa", "idUsuario"],
        "page_size": 50,
        "page_size_limits": {"min": 10, "max": 50},
        "description": "Contratações publicadas por período"
    },
    "contratos": {
        "path": "/v1/contratos",
        "method": "GET",
        "priority": 2,
        "schedule": "daily",
        "required_params": ["dataInicial", "dataFinal", "pagina"],
        "optional_params": ["cnpjOrgao", "codigoUnidadeAdministrativa", "usuarioId"],
        "page_size": 500,
        "page_size_limits": {"min": 10, "max": 500},
        "description": "Contratos/empenhos por período"
    },
    "atas": {
        "path": "/v1/atas",
        "method": "GET",
        "priority": 3,
        "schedule": "daily",
        "required_params": ["dataInicial", "dataFinal", "pagina"],
        "optional_params": ["idUsuario", "cnpj", "codigoUnidadeAdministrativa"],
        "page_size": 500,
        "page_size_limits": {"min": 10, "max": 500},
        "description": "Atas de registro de preços por vigência"
    }
}
```

### **Lógica de Extração MVP:**
1. **Range de datas:** Últimos 30 dias como padrão
2. **Modalidades priorizadas:** Pregão (6,7), Dispensa (8), Concorrência (4,5)
3. **Processamento:** Sequencial com rate limiting básico

## Fase 2B (ETL Completo) - 7 Endpoints Adicionais

### **Endpoints de Sincronização (Atualizações)**

```python
PHASE_2B_SYNC_ENDPOINTS = {
    "contratacoes_atualizacao": {
        "path": "/v1/contratacoes/atualizacao",
        "method": "GET",
        "priority": 4,
        "schedule": "hourly",
        "sync_type": "incremental",
        "watermark_field": "dataAtualizacaoGlobal",
        "required_params": ["dataInicial", "dataFinal", "codigoModalidadeContratacao", "pagina"]
    },
    "contratos_atualizacao": {
        "path": "/v1/contratos/atualizacao",
        "method": "GET",
        "priority": 5,
        "schedule": "hourly",
        "sync_type": "incremental",
        "watermark_field": "dataAtualizacaoGlobal",
        "required_params": ["dataInicial", "dataFinal", "pagina"]
    },
    "atas_atualizacao": {
        "path": "/v1/atas/atualizacao",
        "method": "GET",
        "priority": 6,
        "schedule": "hourly",
        "sync_type": "incremental",
        "watermark_field": "dataAtualizacaoGlobal",
        "required_params": ["dataInicial", "dataFinal", "pagina"]
    }
}
```

### **Endpoints Especializados**

```python
PHASE_2B_SPECIALIZED_ENDPOINTS = {
    "pca_usuario": {
        "path": "/v1/pca/usuario",
        "method": "GET",
        "priority": 7,
        "schedule": "weekly",
        "required_params": ["anoPca", "idUsuario", "pagina"],
        "description": "Planos de contratação por usuário"
    },
    "contratacoes_proposta": {
        "path": "/v1/contratacoes/proposta",
        "method": "GET",
        "priority": 8,
        "schedule": "daily",
        "required_params": ["dataFinal", "pagina"],
        "description": "Contratações com propostas abertas"
    },
    "instrumentos_cobranca": {
        "path": "/v1/instrumentoscobranca/inclusao",
        "method": "GET",
        "priority": 9,
        "schedule": "weekly",
        "required_params": ["dataInicial", "dataFinal", "pagina"],
        "page_size": 100,
        "page_size_limits": {"min": 10, "max": 100},
        "description": "Instrumentos de cobrança"
    },
    "pca_atualizacao": {
        "path": "/v1/pca/atualizacao",
        "method": "GET",
        "priority": 10,
        "schedule": "daily",
        "required_params": ["dataInicio", "dataFim", "pagina"],
        "description": "PCA por data de atualização"
    }
}
```

## Fase 2C (Production) - Endpoints Detalhados

### **Endpoints de Drill-Down**

```python
PHASE_2C_DETAIL_ENDPOINTS = {
    "contratacao_especifica": {
        "path": "/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}",
        "method": "GET",
        "priority": 11,
        "schedule": "on_demand",
        "path_params": ["cnpj", "ano", "sequencial"],
        "description": "Detalhes específicos de contratação",
        "trigger": "missing_details"
    },
    "pca_classificacao": {
        "path": "/v1/pca/",
        "method": "GET",
        "priority": 12,
        "schedule": "monthly",
        "required_params": ["anoPca", "codigoClassificacaoSuperior", "pagina"],
        "description": "PCA por classificação superior"
    }
}
```

## Configuração por Ambiente

### **Desenvolvimento**
```python
DEV_CONFIG = {
    "rate_limit": {
        "requests_per_minute": 30,
        "requests_per_hour": 1000
    },
    "retry": {
        "max_attempts": 3,
        "backoff_factor": 2,
        "backoff_max": 300
    },
    "timeout": {
        "connect": 10,
        "read": 30
    },
    "date_ranges": {
        "default": "last_7_days",
        "max_range": "30_days"
    }
}
```

### **Produção**
```python
PROD_CONFIG = {
    "rate_limit": {
        "requests_per_minute": 60,
        "requests_per_hour": 3000,
        "burst_allowance": 10
    },
    "retry": {
        "max_attempts": 5,
        "backoff_factor": 2,
        "backoff_max": 600,
        "jitter": True
    },
    "timeout": {
        "connect": 15,
        "read": 60
    },
    "date_ranges": {
        "default": "last_30_days",
        "max_range": "365_days"
    }
}
```

## Modalidades de Contratação por Prioridade

### **Alta Prioridade (80% do volume)**
```python
HIGH_PRIORITY_MODALIDADES = [
    6,   # Pregão Eletrônico
    7,   # Pregão Presencial
    8,   # Dispensa de Licitação
    4,   # Concorrência Eletrônica
    5    # Concorrência Presencial
]
```

### **Média Prioridade**
```python
MEDIUM_PRIORITY_MODALIDADES = [
    9,   # Inexigibilidade
    1,   # Leilão Eletrônico
    2,   # Diálogo Competitivo
    13   # Leilão Presencial
]
```

### **Baixa Prioridade**
```python
LOW_PRIORITY_MODALIDADES = [
    3,   # Concurso
    10,  # Manifestação de Interesse
    11,  # Pré-qualificação
    12   # Credenciamento
]
```

## Estratégia de Processamento

### **1. Processamento Paralelo por Modalidade**
```python
# Para contratacoes/publicacao
async def extract_contratacoes_by_modalidade(date_range, modalidades):
    tasks = []
    for modalidade in modalidades:
        task = extract_contratacao_modalidade(date_range, modalidade)
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    return process_results(results)
```

### **2. Chunking de Datas Inteligente**
```python
def get_optimal_date_chunks(start_date, end_date, endpoint_name):
    """Determina chunks ótimos baseado no endpoint"""

    chunk_sizes = {
        "contratacoes_publicacao": 7,    # 7 dias
        "contratos": 15,                 # 15 dias
        "atas": 30,                      # 30 dias
        "instrumentos_cobranca": 30      # 30 dias
    }

    chunk_size = chunk_sizes.get(endpoint_name, 7)
    return create_date_chunks(start_date, end_date, chunk_size)
```

### **3. Circuit Breaker por Endpoint**
```python
endpoint_circuit_breakers = {
    endpoint_name: CircuitBreaker(
        failure_threshold=5,
        recovery_timeout=300,
        expected_exception=HTTPException
    )
    for endpoint_name in ALL_ENDPOINTS
}
```

## Métricas de Monitoramento

### **Métricas por Endpoint**
```python
ENDPOINT_METRICS = {
    "requests_total": "Counter",
    "requests_duration": "Histogram",
    "requests_errors": "Counter",
    "records_extracted": "Counter",
    "pagination_depth": "Histogram",
    "rate_limit_hits": "Counter",
    "circuit_breaker_trips": "Counter"
}
```

### **SLOs (Service Level Objectives)**
```python
ENDPOINT_SLOS = {
    "contratacoes_publicacao": {
        "success_rate": 0.99,       # 99% success
        "p95_latency": 5000,        # 5s p95
        "availability": 0.995       # 99.5% uptime
    },
    "contratos": {
        "success_rate": 0.98,
        "p95_latency": 8000,
        "availability": 0.99
    }
}
```

## Comandos CLI Resultantes

### **Extração por Fase**
```bash
# Fase 2A - MVP
baliza extract --phase 2a --date-range last_30_days

# Fase 2B - Full
baliza extract --phase 2b --date-range 2024-01-01:2024-12-31

# Específico por endpoint
baliza extract --endpoint contratacoes_publicacao --modalidade 6,7,8

# Sincronização incremental
baliza sync --endpoints contratacoes_atualizacao,contratos_atualizacao
```

### **Monitoramento**
```bash
# Status por endpoint
baliza status --endpoints

# Métricas de performance
baliza metrics --endpoint contratacoes_publicacao --period last_24h

# Health check
baliza doctor --check-endpoints
```

---

**Cobertura Total: 12 endpoints mapeados**
**Estratégia: Implementação incremental por fases**
**Priorização: Volume de dados + criticidade de negócio**
