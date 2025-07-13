# BALIZA - Arquitetura Modular

## Princípio de Responsabilidade Única

O BALIZA deve focar exclusivamente na **coleta, armazenamento e preservação** de dados do PNCP. Análises e insights devem ser projetos separados.

## Core BALIZA (Este Repositório)

**Responsabilidade:** Backup e preservação de dados públicos

### ✅ Mantém:
- Coleta automática via PNCP API
- PSA (Persistent Staging Area) com DuckDB
- Upload para Internet Archive
- Arquivos Parquet mensais incrementais
- Monitoramento de execução e qualidade de dados
- DBT básico para limpeza e padronização de dados

### ❌ Remove (para projetos separados):
- Algoritmos de detecção de fraude
- Scoring de contratos suspeitos 
- Análises de risco específicas
- Dashboards analíticos complexos
- Machine learning e AI

## Projetos Complementares (Futuro)

### 1. **baliza-analytics** 
- **Responsabilidade:** Análise de contratos suspeitos
- **Dados:** Consome PSA do BALIZA
- **Funcionalidades:**
  - Algoritmos de scoring (0-100)
  - Detecção de anomalias
  - Análise de padrões suspeitos
  - Alertas automáticos

### 2. **baliza-insights**
- **Responsabilidade:** Dashboards e visualizações
- **Dados:** Consome dados do BALIZA e baliza-analytics
- **Funcionalidades:**
  - KPIs de transparência pública
  - Análise geográfica
  - Benchmarking entre órgãos
  - API pública para desenvolvedores

### 3. **baliza-compliance**
- **Responsabilidade:** Verificação de conformidade legal
- **Dados:** Consome PSA do BALIZA
- **Funcionalidades:**
  - Verificação de regras da Lei 14.133/2021
  - Análise de prazos e procedimentos
  - Relatórios de conformidade

## Interface Entre Projetos

### Protocolo de Dados:
- **BALIZA** expõe PSA via DuckDB read-only
- **Projetos analíticos** conectam via DuckDB attach/federation
- **APIs REST** para consumo externo (futuro)

### Exemplo de Acoplamento:
```python
# No projeto baliza-analytics
import duckdb
conn = duckdb.connect(':memory:')
conn.execute("ATTACH '/path/to/baliza/state/baliza.duckdb' AS baliza_source")
contratos = conn.execute("SELECT * FROM baliza_source.psa.contratos_raw").fetchall()
```

## Benefícios da Separação

1. **Simplicidade:** BALIZA permanece focado e confiável
2. **Evolução:** Projetos analíticos podem evoluir independentemente
3. **Manutenibilidade:** Bugs em análises não afetam coleta de dados
4. **Colaboração:** Diferentes equipes podem trabalhar em paralelos
5. **Compliance:** Core de preservação segue padrões arquivísticos

## Próximos Passos

1. **Limpar BALIZA:** Remover análises complexas do DBT atual
2. **Manter essencial:** Apenas staging/limpeza básica no DBT
3. **Planejar baliza-analytics:** Como projeto separado futuro
4. **Documentar interface:** Como outros projetos devem consumir dados

O BALIZA deve ser a **pedra fundamental confiável** do ecossistema, não uma plataforma monolítica.