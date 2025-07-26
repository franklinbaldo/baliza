# Análise Completa dos Endpoints PNCP API v1.0

**Status**: Todos os 12 endpoints implementados e funcionais.

**Implementação Atual**: O sistema processa todos os endpoints por padrão, com suporte completo a modalidades, paginação inteligente e detecção de lacunas.

## Resumo Executivo

A API PNCP v1.0 oferece **10 endpoints** organizados em **6 categorias** de dados. Todos são endpoints de consulta (GET) com paginação obrigatória.

**Base URL:** `https://pncp.gov.br/api/consulta`

## Mapeamento Completo dos Endpoints

### 1. **PLANOS DE CONTRATAÇÃO ANUAL (PCA)** - 3 endpoints

#### 1.1. `/v1/pca/usuario` - Consultar PCA por Usuário
**Finalidade:** Itens de PCA por ano, usuário e classificação superior
```bash
GET /v1/pca/usuario?anoPca={ano}&idUsuario={id}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `anoPca` (int): Ano do PCA (ex: 2023)
- `idUsuario` (int): ID do sistema que publicou
- `pagina` (int): Número da página (≥1)

**Parâmetros Opcionais:**
- `codigoClassificacaoSuperior` (string): Código da classe/grupo
- `cnpj` (string): CNPJ do órgão
- `tamanhoPagina` (int): 10-500, default 500

#### 1.2. `/v1/pca/atualizacao` - Consultar PCA por Data de Atualização
**Finalidade:** PCA por período de atualização global
```bash
GET /v1/pca/atualizacao?dataInicio={YYYYMMDD}&dataFim={YYYYMMDD}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `dataInicio` (string): Data inicial YYYYMMDD
- `dataFim` (string): Data final YYYYMMDD
- `pagina` (int): Número da página

**Parâmetros Opcionais:**
- `cnpj` (string): CNPJ do órgão
- `codigoUnidade` (string): Código da unidade
- `tamanhoPagina` (int): 10-500, default 500

#### 1.3. `/v1/pca/` - Consultar PCA por Ano e Classificação
**Finalidade:** Itens de PCA por ano e classificação superior
```bash
GET /v1/pca/?anoPca={ano}&codigoClassificacaoSuperior={codigo}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `anoPca` (int): Ano do PCA
- `codigoClassificacaoSuperior` (string): Código obrigatório
- `pagina` (int): Número da página

### 2. **CONTRATAÇÕES** - 4 endpoints

#### 2.1. `/v1/contratacoes/publicacao` - Contratações por Data de Publicação
**Finalidade:** Contratações publicadas em período específico
```bash
GET /v1/contratacoes/publicacao?dataInicial={YYYYMMDD}&dataFinal={YYYYMMDD}&codigoModalidadeContratacao={code}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `dataInicial` (string): Data inicial YYYYMMDD
- `dataFinal` (string): Data final YYYYMMDD
- `codigoModalidadeContratacao` (int): Código da modalidade
- `pagina` (int): Número da página

**Parâmetros Opcionais:**
- `codigoModoDisputa` (int): Modo de disputa
- `uf` (string): Sigla do estado
- `codigoMunicipioIbge` (string): Código IBGE
- `cnpj` (string): CNPJ do órgão
- `codigoUnidadeAdministrativa` (string): Código da unidade
- `idUsuario` (int): ID do usuário
- `tamanhoPagina` (int): 10-50, default 50

#### 2.2. `/v1/contratacoes/proposta` - Contratações com Propostas Abertas
**Finalidade:** Contratações com recebimento de propostas em aberto
```bash
GET /v1/contratacoes/proposta?dataFinal={YYYYMMDD}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `dataFinal` (string): Data final YYYYMMDD
- `pagina` (int): Número da página

**Parâmetros Opcionais:**
- `codigoModalidadeContratacao` (int): Código da modalidade
- `uf` (string): Sigla do estado
- `codigoMunicipioIbge` (string): Código IBGE
- `cnpj` (string): CNPJ do órgão
- `codigoUnidadeAdministrativa` (string): Código da unidade
- `idUsuario` (int): ID do usuário
- `tamanhoPagina` (int): 10-50, default 50

#### 2.3. `/v1/contratacoes/atualizacao` - Contratações por Data de Atualização
**Finalidade:** Contratações por data de atualização global
```bash
GET /v1/contratacoes/atualizacao?dataInicial={YYYYMMDD}&dataFinal={YYYYMMDD}&codigoModalidadeContratacao={code}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `dataInicial` (string): Data inicial YYYYMMDD
- `dataFinal` (string): Data final YYYYMMDD
- `codigoModalidadeContratacao` (int): Código da modalidade
- `pagina` (int): Número da página

**Parâmetros Opcionais:** (mesmos de publicacao)

#### 2.4. `/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}` - Consultar Contratação Específica
**Finalidade:** Detalhes de uma contratação específica
```bash
GET /v1/orgaos/{cnpj}/compras/{ano}/{sequencial}
```

**Parâmetros Obrigatórios:**
- `cnpj` (string): CNPJ do órgão
- `ano` (int): Ano da contratação
- `sequencial` (int): Número sequencial (≥1)

### 3. **ATAS DE REGISTRO DE PREÇOS** - 2 endpoints

#### 3.1. `/v1/atas` - Atas por Período de Vigência
**Finalidade:** Atas por período de vigência
```bash
GET /v1/atas?dataInicial={YYYYMMDD}&dataFinal={YYYYMMDD}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `dataInicial` (string): Data inicial YYYYMMDD
- `dataFinal` (string): Data final YYYYMMDD
- `pagina` (int): Número da página

**Parâmetros Opcionais:**
- `idUsuario` (int): ID do usuário
- `cnpj` (string): CNPJ do órgão
- `codigoUnidadeAdministrativa` (string): Código da unidade
- `tamanhoPagina` (int): 10-500, default 500

#### 3.2. `/v1/atas/atualizacao` - Atas por Data de Atualização
**Finalidade:** Atas por data de atualização global
```bash
GET /v1/atas/atualizacao?dataInicial={YYYYMMDD}&dataFinal={YYYYMMDD}&pagina={page}
```

**Parâmetros:** (mesmos de `/v1/atas`)

### 4. **CONTRATOS/EMPENHOS** - 2 endpoints

#### 4.1. `/v1/contratos` - Contratos por Data de Publicação
**Finalidade:** Contratos/empenhos por período de publicação
```bash
GET /v1/contratos?dataInicial={YYYYMMDD}&dataFinal={YYYYMMDD}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `dataInicial` (string): Data inicial YYYYMMDD
- `dataFinal` (string): Data final YYYYMMDD
- `pagina` (int): Número da página

**Parâmetros Opcionais:**
- `cnpjOrgao` (string): CNPJ do órgão
- `codigoUnidadeAdministrativa` (string): Código da unidade
- `usuarioId` (int): ID do usuário
- `tamanhoPagina` (int): 10-500, default 500

#### 4.2. `/v1/contratos/atualizacao` - Contratos por Data de Atualização
**Finalidade:** Contratos por data de atualização global
```bash
GET /v1/contratos/atualizacao?dataInicial={YYYYMMDD}&dataFinal={YYYYMMDD}&pagina={page}
```

**Parâmetros:** (mesmos de `/v1/contratos`)

### 5. **INSTRUMENTOS DE COBRANÇA** - 1 endpoint

#### 5.1. `/v1/instrumentoscobranca/inclusao` - Instrumentos por Data de Inclusão
**Finalidade:** Instrumentos de cobrança por período de inclusão
```bash
GET /v1/instrumentoscobranca/inclusao?dataInicial={YYYYMMDD}&dataFinal={YYYYMMDD}&pagina={page}
```

**Parâmetros Obrigatórios:**
- `dataInicial` (string): Data inicial YYYYMMDD
- `dataFinal` (string): Data final YYYYMMDD
- `pagina` (int): Número da página

**Parâmetros Opcionais:**
- `tipoInstrumentoCobranca` (int): Tipo do instrumento
- `cnpjOrgao` (string): CNPJ do órgão
- `tamanhoPagina` (int): 10-100, default 100

## Estratégia de Extração por Endpoint

### **Prioridade Alta (Dados Principais)**
1. **`/v1/contratacoes/publicacao`** - Core das contratações
2. **`/v1/contratos`** - Contratos efetivados
3. **`/v1/atas`** - Atas de registro de preços

### **Prioridade Média (Atualizações)**
4. **`/v1/contratacoes/atualizacao`** - Para sincronização
5. **`/v1/contratos/atualizacao`** - Para sincronização
6. **`/v1/atas/atualizacao`** - Para sincronização

### **Prioridade Baixa (Especializados)**
7. **`/v1/pca/*`** - Planos de contratação
8. **`/v1/contratacoes/proposta`** - Propostas abertas
9. **`/v1/instrumentoscobranca/inclusao`** - Instrumentos cobrança
10. **`/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}`** - Detalhamento específico

## Padrões Identificados

### **Paginação Obrigatória**
- Todos endpoints requerem parâmetro `pagina` (≥1)
- Retorna: `totalRegistros`, `totalPaginas`, `numeroPagina`, `paginasRestantes`, `empty`

### **Filtros de Data**
- **Por publicação:** `dataInicial` + `dataFinal`
- **Por atualização:** `dataInicial` + `dataFinal`
- **Por vigência:** `dataInicial` + `dataFinal`
- **Formato:** YYYYMMDD (ex: 20240115)

### **Filtros Opcionais Comuns**
- `cnpj` / `cnpjOrgao`: CNPJ do órgão
- `codigoUnidadeAdministrativa`: Código da unidade
- `idUsuario` / `usuarioId`: ID do sistema publicador
- `tamanhoPagina`: Controle de paginação

### **Códigos HTTP**
- `200`: Sucesso com dados
- `204`: Sucesso sem dados
- `400`: Bad Request
- `422`: Unprocessable Entity
- `500`: Internal Server Error

## Tabelas de Domínio Críticas

### **Modalidades de Contratação** (obrigatório em alguns endpoints)
- 1: Leilão Eletrônico
- 2: Diálogo Competitivo
- 3: Concurso
- 4: Concorrência Eletrônica
- 5: Concorrência Presencial
- 6: Pregão Eletrônico
- 7: Pregão Presencial
- 8: Dispensa de Licitação
- 9: Inexigibilidade
- 10: Manifestação de Interesse
- 11: Pré-qualificação
- 12: Credenciamento
- 13: Leilão Presencial

### **Situações da Contratação**
- 1: Divulgada no PNCP
- 2: Revogada
- 3: Anulada
- 4: Suspensa

## Recomendações para Implementação

### **1. Extração Incremental**
- Usar endpoints `/atualizacao` para sincronização
- Manter watermark da última extração
- Processar apenas deltas quando possível

### **2. Rate Limiting**
- Implementar delays entre requests
- Monitorar headers de rate limit (se existirem)
- Circuit breaker para falhas sucessivas

### **3. Retry Strategy**
- 429 (Rate Limit): Exponential backoff
- 500+ (Server Error): Retry com backoff
- 400/422 (Client Error): Não retry

### **4. Estrutura de Dados**
- Schema único por endpoint type
- Normalização de campos comuns
- Preservação de estruturas aninhadas (JSON)

### **5. Monitoramento**
- Track de success rate por endpoint
- Latência média por endpoint
- Volume de dados por período
- Detecção de mudanças de schema

---

**Total de endpoints mapeados: 10**
**Cobertura: 100% da API PNCP v1.0**
