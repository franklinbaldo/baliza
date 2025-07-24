# Estratégia de Extração Concorrente PNCP

## Descoberta: SEM Rate Limiting Tradicional

### **🔍 Análise da Documentação**
- ❌ Sem limites explícitos de requests/minuto
- ❌ Sem códigos HTTP 429 documentados  
- ❌ Sem headers de rate limiting
- ✅ Controle apenas por paginação e capacidade do servidor

### **🚀 Oportunidade de Performance**

**TODOS os 12 endpoints podem rodar SIMULTANEAMENTE!**

## Estratégia de Concorrência Otimizada  

### **1. Extração Paralela por Endpoint**

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

async def extract_all_endpoints_concurrent(date_range):
    """Extrai de todos os endpoints simultaneamente"""
    
    endpoints = [
        "contratacoes_publicacao",
        "contratacoes_atualizacao", 
        "contratacoes_proposta",
        "contratos",
        "contratos_atualizacao",
        "atas",
        "atas_atualizacao",
        "pca_usuario",
        "pca_atualizacao", 
        "pca_classificacao",
        "instrumentos_cobranca",
        "contratacao_especifica"  # On-demand
    ]
    
    # Criar tasks para todos os endpoints
    tasks = []
    for endpoint in endpoints:
        if should_extract_endpoint(endpoint, date_range):
            task = extract_endpoint_data(endpoint, date_range)
            tasks.append(task)
    
    # Executar todos em paralelo
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return process_concurrent_results(results)
```

### **2. Paralelização por Modalidade**

```python
async def extract_contratacoes_all_modalidades(date_range):
    """Extrai contratações de todas as modalidades em paralelo"""
    
    modalidades = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    
    # Uma task por modalidade
    tasks = []
    for modalidade in modalidades:
        task = extract_contratacao_modalidade(date_range, modalidade)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return merge_modalidade_results(results)
```

### **3. Chunking Temporal Inteligente**

```python
def get_optimal_chunks(endpoint_name, date_range):
    """Chunks otimizados baseados no volume esperado"""
    
    # Chunks menores para endpoints com mais dados
    chunk_strategies = {
        "contratacoes_publicacao": 3,    # 3 dias (alto volume)
        "contratos": 7,                  # 7 dias (médio volume)
        "atas": 15,                      # 15 dias (baixo volume)
        "instrumentos_cobranca": 30,     # 30 dias (muito baixo volume)
        "pca_*": 90                      # 90 dias (dados anuais)
    }
    
    chunk_days = chunk_strategies.get(endpoint_name, 7)
    return create_date_chunks(date_range, chunk_days)
```

### **4. Pool de Conexões Otimizado**

```python
import httpx

# Configuração para alta concorrência
HTTP_CONFIG = {
    "limits": httpx.Limits(
        max_keepalive_connections=20,   # Pool persistente
        max_connections=100,            # Total de conexões
        keepalive_expiry=60             # Keep-alive por 60s
    ),
    "timeout": httpx.Timeout(
        connect=10.0,
        read=30.0,
        write=10.0,
        pool=5.0
    ),
    "http2": True,                      # HTTP/2 para melhor multiplexing
    "retries": 3
}

async def create_optimized_client():
    return httpx.AsyncClient(**HTTP_CONFIG)
```

## Performance Estimada vs Atual

### **❌ Abordagem Sequencial (Original)**
```python
# 30 requests/minuto sequencial
estimated_time_sequential = {
    "contratacoes_publicacao": "~45 min",  # 5 modalidades × 30 dias × 3 páginas
    "contratos": "~15 min",                # 30 dias × 2 páginas  
    "atas": "~10 min",                     # 30 dias × 1 página
    "total_per_month": "~70 minutos"
}
```

### **✅ Abordagem Concorrente (Nova)**
```python
# 120 requests/minuto × 12 endpoints paralelos
estimated_time_concurrent = {
    "all_endpoints_parallel": "~8 min",    # Todos rodando juntos
    "total_per_month": "~8 minutos",       # 87% mais rápido!
    "throughput_improvement": "8.75x"
}
```

## Implementação Prática

### **1. Comando CLI Otimizado**
```bash
# Extração concorrente de todos os endpoints
baliza extract --concurrent --all-endpoints --date-range last_30_days

# Específico por prioridade
baliza extract --concurrent --priority high --modalidades 6,7,8

# Com controle de concorrência
baliza extract --concurrent --max-workers 12 --requests-per-minute 120
```

### **2. Monitoramento de Performance**
```python
CONCURRENT_METRICS = {
    "endpoint_throughput": "requests/second per endpoint",
    "parallel_efficiency": "actual_time / theoretical_sequential_time", 
    "server_response_time": "p95 latency per endpoint",
    "concurrent_errors": "errors due to server overload",
    "bandwidth_utilization": "MB/s during extraction"
}
```

### **3. Circuit Breaker Inteligente**
```python
class AdaptiveCircuitBreaker:
    """Circuit breaker que se adapta à resposta do servidor"""
    
    def __init__(self):
        self.failure_threshold = 5
        self.recovery_timeout = 300
        self.adaptive_backoff = True
        
    def should_trip(self, endpoint_name, error_rate, response_time):
        """Decide se deve parar baseado em múltiplas métricas"""
        
        # Trip mais rápido para endpoints críticos
        if endpoint_name in ["contratacoes_publicacao"]:
            return error_rate > 0.1 or response_time > 10000
        
        # Mais tolerante para endpoints secundários  
        return error_rate > 0.2 or response_time > 20000
```

## Configuração de Produção Otimizada

### **Desenvolvimento (Conservador)**
```python
DEV_CONCURRENT_CONFIG = {
    "max_concurrent_endpoints": 3,
    "requests_per_minute": 60,
    "chunk_size_days": 7,
    "max_retries": 3
}
```

### **Produção (Agressivo)**
```python
PROD_CONCURRENT_CONFIG = {
    "max_concurrent_endpoints": 12,     # Todos simultaneamente
    "requests_per_minute": 120,         # Limite conservador
    "requests_per_hour": 7200,          # 120 * 60
    "chunk_size_days": 3,               # Chunks menores
    "max_retries": 5,
    "adaptive_rate_limiting": True,     # Reduz se detectar sobrecarga
    "connection_pool_size": 20,
    "http2_enabled": True
}
```

## Riscos e Mitigações

### **⚠️ Riscos Identificados**
1. **Sobrecarga do servidor PNCP** → Monitor 500 errors, backoff adaptativo
2. **Consumo de memória local** → Streaming + batch processing  
3. **Rate limiting implícito** → Circuit breaker + graceful degradation
4. **Inconsistência de dados** → Timestamps + checksums de integridade

### **🛡️ Mitigações**
```python
SAFETY_MEASURES = {
    "server_health_monitoring": "Stop if >10% 500 errors",
    "memory_limits": "Max 2GB RAM usage per worker",
    "graceful_degradation": "Fall back to sequential if concurrent fails",
    "data_validation": "Schema validation + duplicate detection",
    "checkpoint_recovery": "Resume from last successful chunk"
}
```

## Resultado Esperado

### **🎯 Performance Target**
- **Throughput**: 8.75x mais rápido que abordagem sequencial
- **Tempo total**: ~8 minutos para mês completo (vs 70 minutos)
- **Eficiência**: 87% redução no tempo de extração
- **Cobertura**: 100% dos endpoints simultaneamente

### **📊 KPIs de Sucesso**
- Success rate > 99% em todos os endpoints
- P95 latency < 10s por request  
- Zero rate limiting (HTTP 429) errors
- Memory usage < 2GB durante extração
- Database ingestion rate > 1000 records/second

---

**🚀 CONCLUSÃO: Podemos extrair TODOS os endpoints SIMULTANEAMENTE com performance 8.75x superior, mantendo a estabilidade através de circuit breakers adaptativos e monitoramento proativo!**