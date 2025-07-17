# Plano de Modularização do BALIZA (Simplificado)

## Visão Geral

Este documento descreve o plano simplificado para organizar o código do BALIZA em módulos lógicos, mantendo uma estrutura plana e direta, sem subpastas dentro de `src/baliza/`.

## Estrutura Proposta

```
baliza/
├── src/
│   └── baliza/                   # Pacote principal
│       ├── __init__.py           # Inicialização do pacote
│       ├── cli.py                # Comandos da linha de comando
│       ├── extractor.py          # Lógica de extração de dados
│       ├── models.py             # Modelos e esquemas de dados
│       ├── services.py           # Integrações com serviços externos
│       └── utils.py              # Utilitários gerais
│
├── tests/                        # Testes automatizados
│   ├── test_extractor.py
│   ├── test_services.py
│   └── ...
│
├── dbt_baliza/                   # Transformações dbt
│   ├── models/
│   └── ...
│
└── data/                         # Dados locais
    └── .gitkeep
```

## Mapeamento de Arquivos Atuais

| Caminho Atual | Novo Caminho | Observações |
|--------------|-------------|-------------|
| `src/baliza/pncp_extractor.py` | `src/baliza/extractor.py` | Lógica principal de extração |
| `src/baliza/cli.py` | `src/baliza/cli.py` | Manter e organizar melhor |
| Configurações | `src/baliza/models.py` | Centralizar em models.py |
| Utilitários | `src/baliza/utils.py` | Todas as funções auxiliares |
| Integrações | `src/baliza/services.py` | Toda lógica de serviços externos |

## Detalhamento dos Módulos

### 1. `cli.py`

**Responsabilidades:**
- Interpretar argumentos de linha de comando
- Orquestrar o fluxo de execução
- Exibir saída formatada para o usuário

**Conteúdo:**
- Definição dos comandos principais (extract, stats)
- Tratamento de argumentos e opções
- Formatação de saída

### 2. `extractor.py`

**Responsabilidades:**
- Gerenciar a extração de dados do PNCP
- Controlar concorrência
- Tratar erros e retentativas

**Conteúdo:**
- Classe principal de extração
- Gerenciamento de sessões
- Lógica de paginação

### 3. `models.py`

**Responsabilidades:**
- Definir estruturas de dados
- Gerenciar configurações
- Validar dados

**Conteúdo:**
- Classes de modelo de dados
- Esquemas de validação
- Configurações do sistema

### 4. `services.py`

**Responsabilidades:**
- Comunicar-se com serviços externos
- Gerenciar autenticação
- Tratar erros de rede

**Conteúdo:**
- Cliente da API PNCP
- Integração com Internet Archive
- Outras integrações necessárias

### 5. `utils.py`

**Responsabilidades:**
- Fornecer funções auxiliares
- Gerenciar logs
- Tratar erros comuns

**Conteúdo:**
- Funções de utilidade geral
- Configuração de logging
- Classes de erro personalizadas

## Plano de Migração

### Fase 1: Reorganização (3-5 dias)

1. **Criar a nova estrutura**
   - Criar os novos arquivos principais
   - Mover código existente para os novos módulos
   - Atualizar imports

2. **Consolidar código**
   - Juntar funções relacionadas
   - Remover duplicações
   - Padronizar nomes

### Fase 2: Refatoração (1-2 semanas)

1. **Organizar o código**
   - Agrupar funções relacionadas
   - Melhorar nomes de variáveis e funções
   - Adicionar documentação

2. **Melhorar tratamento de erros**
   - Padronizar mensagens de erro
   - Adicionar logs úteis
   - Melhorar feedback ao usuário

### Fase 3: Testes (3-5 dias)

1. **Testes Unitários**
   - Criar testes para funções principais
   - Testar casos de borda
   - Garantir cobertura básica

2. **Testes de Integração**
   - Testar fluxos principais
   - Validar comunicação entre módulos

### Fase 4: Documentação (1-2 dias)

1. **Atualizar documentação**
   - Atualizar README
   - Documentar a nova estrutura
   - Criar exemplos de uso

## Benefícios da Nova Estrutura

- **Simplicidade**: Menos camadas e mais direto ao ponto
- **Facilidade de Navegação**: Tudo visível no mesmo nível
- **Manutenção Simplificada**: Menos arquivos para gerenciar
- **Entrada Mais Rápida**: Nova equipe consegue entender mais rapidamente

## Próximos Passos

1. Validar a estrutura proposta
2. Iniciar a migração gradual
3. Atualizar documentação
4. Realizar testes de regressão
