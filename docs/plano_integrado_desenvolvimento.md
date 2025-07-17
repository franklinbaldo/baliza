# Plano Integrado de Desenvolvimento - BALIZA ETL + Modularização

## Visão Geral Executiva

Este documento apresenta um plano integrado que combina a implementação do pipeline ETL completo (Extract-Transform-Load) com a modularização do código BALIZA. O objetivo é criar uma arquitetura robusta, escalável e maintível que atenda aos requisitos de dados abertos para licitações públicas brasileiras.

## Objetivos Estratégicos

### 1. Pipeline ETL Completo
- ✅ **Extract**: Dados brutos do PNCP (já implementado)
- 🔄 **Transform**: Processamento e enriquecimento com dbt
- 🔄 **Load**: Publicação no Internet Archive

### 2. Modularização do Código
- 🔄 Estrutura de código limpa e organizada
- 🔄 Separação clara de responsabilidades
- 🔄 Facilidade de manutenção e evolução

### 3. Qualidade e Sustentabilidade
- 🔄 Testes automatizados abrangentes
- 🔄 Documentação completa
- 🔄 Práticas de desenvolvimento modernas

## Arquitetura Integrada Proposta

### Estrutura Final do Projeto
```
baliza/
├── src/
│   └── baliza/                   # Pacote principal modularizado
│       ├── __init__.py           # Inicialização e configuração
│       ├── cli.py                # Interface de linha de comando unificada
│       ├── extractor.py          # Lógica de extração (refatorado)
│       ├── transformer.py        # Lógica de transformação dbt
│       ├── loader.py             # Lógica de carregamento IA
│       ├── models.py             # Modelos de dados e validação
│       ├── services.py           # Serviços externos (PNCP, IA)
│       └── utils.py              # Utilitários e logging
│
├── dbt_baliza/                   # Projeto dbt existente
│   ├── models/
│   │   ├── bronze/               # Dados brutos parseados
│   │   ├── silver/               # Dados limpos e normalizados
│   │   └── gold/                 # Datasets analíticos
│   ├── macros/                   # Macros dbt personalizadas
│   ├── tests/                    # Testes de qualidade dbt
│   └── docs/                     # Documentação dbt
│
├── templates/                    # Templates para metadados
│   ├── archive_metadata.json.j2
│   ├── collection_description.md.j2
│   └── data_dictionary.md.j2
│
├── tests/                        # Testes automatizados
│   ├── unit/                     # Testes unitários
│   ├── integration/              # Testes de integração
│   └── e2e/                      # Testes end-to-end
│
├── docs/                         # Documentação técnica
│   ├── api/                      # Documentação da API
│   ├── deployment/               # Guias de deployment
│   └── user_guide/               # Guias do usuário
│
├── config/                       # Configurações
│   ├── settings.yaml             # Configurações principais
│   ├── logging.yaml              # Configuração de logging
│   └── archive_config.yaml       # Configuração Internet Archive
│
└── data/                         # Dados locais
    ├── raw/                      # Dados brutos
    ├── processed/                # Dados processados
    └── exports/                  # Dados para export
```

## Detalhamento dos Módulos

### 1. `cli.py` - Interface Unificada
**Responsabilidades:**
- Comandos `extract`, `transform`, `load`
- Orquestração do pipeline completo
- Feedback ao usuário com progress bars
- Configuração e validação de argumentos

**Comandos Principais:**
```bash
baliza extract                    # Extração de dados PNCP
baliza transform                  # Transformação com dbt
baliza load                       # Carregamento para IA
baliza pipeline                   # Pipeline completo
baliza stats                      # Estatísticas e status
```

### 2. `extractor.py` - Extração Robusta
**Responsabilidades:**
- Extração assíncrona de dados PNCP
- Controle de concorrência e rate limiting
- Recuperação de falhas e retry logic
- Armazenamento em DuckDB otimizado

**Melhorias:**
- Melhor tratamento de erros
- Logs estruturados
- Métricas de performance
- Configuração flexível

### 3. `transformer.py` - Transformação Inteligente
**Responsabilidades:**
- Parsing de JSON bruto da tabela `pncp_raw_responses`
- Execução e orquestração de transformações dbt
- Validação de dados com pydantic
- Geração de datasets analíticos

**Funcionalidades:**
- Processamento incremental
- Validação de qualidade
- Enrichment de dados
- Particionamento inteligente

### 4. `loader.py` - Carregamento Eficiente
**Responsabilidades:**
- Exportação para múltiplos formatos (Parquet, CSV, JSON)
- Geração de metadados automática
- Upload para Internet Archive
- Controle de versões e incrementos

**Funcionalidades:**
- Compressão otimizada
- Metadata rico
- Upload resumível
- Versionamento semântico

### 5. `models.py` - Estruturas de Dados
**Responsabilidades:**
- Modelos Pydantic para validação
- Esquemas de dados padronizados
- Configurações tipadas
- Constantes e enumerações

### 6. `services.py` - Integrações Externas
**Responsabilidades:**
- Cliente HTTP para API PNCP
- Cliente Internet Archive
- Autenticação e autorização
- Tratamento de erros de rede

### 7. `utils.py` - Utilitários Compartilhados
**Responsabilidades:**
- Logging estruturado
- Funções de data/hora
- Helpers de arquivo
- Tratamento de exceções

## Plano de Implementação Detalhado

### MILESTONE 1: Preparação e Estruturação (1-2 semanas)
**Objetivos**: Preparar base sólida para desenvolvimento

#### M1.1: Reorganização do Código (3-5 dias)
- [ ] **Criar nova estrutura de diretórios**
  - Criar todos os diretórios da arquitetura proposta
  - Mover arquivos existentes para nova estrutura
  - Atualizar imports e referências

- [ ] **Refatorar pncp_extractor.py → extractor.py**
  - Manter toda funcionalidade existente
  - Melhorar organização interna
  - Adicionar logs estruturados
  - Documentar classes e métodos

- [ ] **Consolidar CLI**
  - Integrar comandos existentes
  - Padronizar interface
  - Adicionar validação de argumentos
  - Melhorar mensagens de erro

#### M1.2: Configuração e Dependências (1-2 dias)
- [ ] **Atualizar pyproject.toml**
  - Verificar dependências ETL
  - Adicionar dependências de teste
  - Configurar grupos opcionais
  - Atualizar metadados do projeto

- [ ] **Criar sistema de configuração**
  - Arquivo `config/settings.yaml`
  - Carregamento de configurações
  - Validação com pydantic
  - Suporte a variáveis de ambiente

#### M1.3: Logging e Monitoramento (1-2 dias)
- [ ] **Implementar logging estruturado**
  - Configuração centralizada
  - Níveis de log apropriados
  - Formatação consistente
  - Rotação de logs

- [ ] **Adicionar métricas básicas**
  - Tempo de execução
  - Contadores de sucesso/erro
  - Uso de recursos
  - Progresso de operações

### MILESTONE 2: Implementação Transform (2-3 semanas)
**Objetivos**: Comando `baliza transform` completamente funcional

#### M2.1: Parsing de Dados Brutos (1 semana)
- [ ] **Implementar parser JSON**
  - Ler dados de `pncp_raw_responses`
  - Validar estrutura JSON
  - Tratar dados malformados
  - Logging de erros de parsing

- [ ] **Modelos Pydantic**
  - Definir schemas para todos os tipos de dados
  - Validação automática
  - Serialização/deserialização
  - Documentação automática

- [ ] **Preparação para dbt**
  - Criação de views/tabelas staging
  - Limpeza de dados
  - Normalização de formatos
  - Detecção de duplicatas

#### M2.2: Integração dbt Robusta (1 semana)
- [ ] **Orquestração dbt**
  - Execução programática
  - Tratamento de erros
  - Configuração dinâmica
  - Paralelização otimizada

- [ ] **Validação de Qualidade**
  - Testes de dados automáticos
  - Verificação de consistência
  - Detecção de anomalias
  - Relatórios de qualidade

- [ ] **Processamento Incremental**
  - Detecção de mudanças
  - Processamento apenas de novos dados
  - Otimização de performance
  - Controle de dependências

#### M2.3: Enriquecimento de Dados (3-5 dias)
- [ ] **Cálculos derivados**
  - Métricas agregadas
  - Indicadores de performance
  - Classificações automáticas
  - Geolocalização

- [ ] **Padronização**
  - Normalização de nomes
  - Códigos padronizados
  - Formatos consistentes
  - Limpeza de dados

### MILESTONE 3: Implementação Load (2-3 semanas)
**Objetivos**: Comando `baliza load` totalmente funcional

#### M3.1: Exportação de Dados (1 semana)
- [ ] **Múltiplos formatos**
  - Parquet (formato principal)
  - CSV para compatibilidade
  - JSON para flexibilidade
  - Compressão otimizada

- [ ] **Organização de arquivos**
  - Estrutura de diretórios lógica
  - Nomenclatura consistente
  - Particionamento por data
  - Indexação automática

- [ ] **Validação de exports**
  - Verificação de integridade
  - Validação de schemas
  - Testes de qualidade
  - Relatórios de export

#### M3.2: Sistema de Metadados (1 semana)
- [ ] **Templates Jinja2**
  - Metadados Archive.org
  - Descrições de coleções
  - Dicionários de dados
  - Documentação automática

- [ ] **Geração automática**
  - Estatísticas de datasets
  - Schemas em JSON
  - Documentação markdown
  - Changelog automático

- [ ] **Versionamento**
  - Controle de versões semântico
  - Tracking de mudanças
  - Compatibilidade backward
  - Histórico de releases

#### M3.3: Internet Archive Integration (1 semana)
- [ ] **Cliente Internet Archive**
  - Autenticação robusta
  - Upload resumível
  - Tratamento de erros
  - Progress tracking

- [ ] **Gestão de coleções**
  - Criação automática
  - Organização por categorias
  - Metadados ricos
  - Indexação para busca

- [ ] **Upload incremental**
  - Detecção de mudanças
  - Upload apenas de novos dados
  - Verificação de integridade
  - Rollback em caso de erro

### MILESTONE 4: Testes e Qualidade (2-3 semanas)
**Objetivos**: Cobertura de testes abrangente e qualidade assegurada

#### M4.1: Testes Unitários (1 semana)
- [ ] **Cobertura básica**
  - Todos os módulos principais
  - Funções críticas
  - Casos de borda
  - Mocks apropriados

- [ ] **Testes de validação**
  - Schemas Pydantic
  - Parsing de dados
  - Transformações
  - Exports

#### M4.2: Testes de Integração (1 semana)
- [ ] **Fluxos completos**
  - Extract → Transform
  - Transform → Load
  - Pipeline completo
  - Recuperação de falhas

- [ ] **Integrações externas**
  - API PNCP (com mocks)
  - Internet Archive (sandbox)
  - dbt execução
  - DuckDB operações

#### M4.3: Testes End-to-End (1 semana)
- [ ] **Cenários reais**
  - Pipeline completo
  - Dados de produção
  - Casos de erro
  - Performance

- [ ] **Automatização**
  - CI/CD pipeline
  - Testes automatizados
  - Deployment automático
  - Monitoring contínuo

### MILESTONE 5: Documentação e Deployment (1-2 semanas)
**Objetivos**: Documentação completa e deployment production-ready

#### M5.1: Documentação Técnica (1 semana)
- [ ] **API Documentation**
  - Docstrings completas
  - Exemplos de uso
  - Referência de comandos
  - Configuração detalhada

- [ ] **Guias de usuário**
  - Instalação e setup
  - Workflows comuns
  - Troubleshooting
  - Exemplos práticos

#### M5.2: Deployment e Operações (3-5 dias)
- [ ] **Containerização**
  - Dockerfile otimizado
  - Docker compose
  - Configuração de ambiente
  - Volumes e persistência

- [ ] **Monitoramento**
  - Logs estruturados
  - Métricas de performance
  - Alertas automáticos
  - Dashboards

### MILESTONE 6: Lançamento e Manutenção (Contínuo)
**Objetivos**: Lançamento público e manutenção contínua

#### M6.1: Lançamento Público (1 semana)
- [ ] **Preparação**
  - Testes finais
  - Documentação revisada
  - Comunicação pública
  - Suporte inicial

- [ ] **Dados iniciais**
  - Upload dataset completo
  - Verificação de qualidade
  - Indexação Archive.org
  - Anúncio público

#### M6.2: Manutenção Contínua
- [ ] **Monitoramento**
  - Execução regular
  - Qualidade de dados
  - Performance
  - Erros e falhas

- [ ] **Atualizações**
  - Melhorias contínuas
  - Correção de bugs
  - Novos recursos
  - Feedback da comunidade

## Cronograma Estimado

### Resumo por Milestones
| Milestone | Duração | Equipe | Prioridade |
|-----------|---------|---------|------------|
| M1: Preparação | 1-2 semanas | 1 dev | Crítica |
| M2: Transform | 2-3 semanas | 1 dev | Crítica |
| M3: Load | 2-3 semanas | 1 dev | Crítica |
| M4: Testes | 2-3 semanas | 1 dev | Alta |
| M5: Docs | 1-2 semanas | 1 dev | Média |
| M6: Lançamento | 1 semana | 1 dev | Alta |

### Cronograma Total
- **Duração total**: 10-15 semanas (2.5-3.5 meses)
- **Equipe necessária**: 1 desenvolvedor sênior
- **Recursos**: Servidor/ambiente desenvolvimento, credenciais Archive.org

### Faseamento Paralelo
```
Semana 1-2:   [M1: Preparação        ]
Semana 3-5:   [M2: Transform         ]
Semana 6-8:   [M3: Load              ]
Semana 9-11:  [M4: Testes            ]
Semana 12-13: [M5: Documentação      ]
Semana 14:    [M6: Lançamento        ]
```

## Riscos e Mitigações

### Riscos Técnicos
| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|-----------|
| Problemas API PNCP | Alta | Médio | Retry logic, fallbacks |
| Limitações Internet Archive | Média | Alto | Testes extensivos, suporte oficial |
| Performance dbt | Média | Médio | Otimização, paralelização |
| Problemas de memória | Média | Alto | Processamento em lotes |

### Riscos de Projeto
| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|-----------|
| Mudanças de escopo | Média | Alto | Definição clara, marcos |
| Recursos insuficientes | Baixa | Alto | Planejamento detalhado |
| Dependências externas | Média | Médio | Alternatives, contingências |

## Métricas de Sucesso

### Técnicas
- **Cobertura de testes**: >90%
- **Performance**: Extract <4h, Transform <30min, Load <1h
- **Qualidade**: 0 erros críticos, <5% dados rejeitados
- **Disponibilidade**: 99.9% uptime

### Negócio
- **Dados públicos**: 100% dados PNCP desde 2021
- **Formatos**: Parquet, CSV, JSON
- **Atualizações**: Mensais automáticas
- **Uso**: Métricas de download/uso

### Qualidade
- **Documentação**: 100% APIs documentadas
- **Testes**: E2E, integração, unitários
- **Manutenibilidade**: Código limpo, modular
- **Usabilidade**: Comandos simples, logs claros

## Recursos Necessários

### Humanos
- **1 Desenvolvedor Sênior Python** (tempo integral)
- **1 DevOps/SRE** (suporte pontual)
- **1 Product Owner** (definição requisitos)

### Técnicos
- **Servidor desenvolvimento**: 16GB RAM, 8 CPUs
- **Armazenamento**: 500GB SSD para dados
- **Credenciais**: Internet Archive, APIs necessárias
- **Ferramentas**: GitHub, Docker, monitoring

### Orçamento Estimado
- **Desenvolvimento**: 3 meses desenvolvedor sênior
- **Infraestrutura**: Servidor cloud + storage
- **Serviços**: Internet Archive, monitoring
- **Total**: Orçamento para 3-4 meses operação

## Conclusão

Este plano integrado combina a necessidade de modularização do código com a implementação do pipeline ETL completo, criando uma base sólida para o futuro do projeto BALIZA. A abordagem faseada permite entregas incrementais e reduz riscos, enquanto a arquitetura modular garante manutenibilidade e escalabilidade.

### Próximos Passos Imediatos
1. **Aprovar o plano** e alocação de recursos
2. **Iniciar Milestone 1** - Preparação e estruturação
3. **Setup ambiente** desenvolvimento e ferramentas
4. **Definir métricas** de acompanhamento detalhadas

### Impacto Esperado
- **Dados abertos**: Democratização acesso dados licitações
- **Transparência**: Histórico completo procurement brasileiro
- **Pesquisa**: Facilitar análises acadêmicas e jornalísticas
- **Tecnologia**: Referência pipeline ETL dados governamentais

---

**Documento versão**: 1.0  
**Data**: Janeiro 2025  
**Autor**: Equipe BALIZA  
**Próxima revisão**: Após conclusão Milestone 1