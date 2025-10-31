# Plano de limpeza do repositório

Este documento acompanha o esforço de redução de legado e simplificação do
Baliza após a migração para o pipeline declarativo com dlt. O objetivo é manter
apenas os componentes necessários para extrair dados do PNCP e entregá-los em
formato analítico.

## Estado atual

- O pipeline declarativo (`src/baliza/pipelines/pncp.py`) já orquestra a
  extração com dlt e grava os dados no DuckDB local.
- Código legado desconectado foi removido, e o repositório agora expõe apenas
  os módulos utilizados pela CLI (`cli.py`, `pipelines/`, `utils/dates.py`).
- A suíte de testes end-to-end cobre a CLI principal, mas ainda faltam casos
  unitários para utilitários críticos.

## Ações concluídas ✅

- Criação da configuração declarativa `config/pncp.yml` para o endpoint de
  contratos.
- Substituição do fluxo Prefect pelo comando `baliza extract` baseado em dlt.
- Documentação do fluxo incremental e dos parâmetros de execução na nova
  `README.md`.
- Remoção dos módulos legados `baliza.extraction`, `baliza.settings` e
  utilitários não utilizados.
- Redução das dependências do projeto às bibliotecas efetivamente utilizadas.

## Próximos passos

### 1. Atualizar e expandir testes

- Recriar um teste end-to-end mínimo que execute `run_pncp` com um mock do
  endpoint e valide a escrita no DuckDB.
- Adicionar testes unitários para `_apply_incremental_overrides` e outras
  funções utilitárias críticas.

### 2. Remover documentação obsoleta

- Substituir referências à arquitetura antiga (Prefect, múltiplos comandos)
  pelos planos descritos nos documentos atualizados em `docs/`.
- Garantir que novos PRs atualizem sempre README e documentos técnicos quando
  introduzirem funcionalidades.

### 3. Automatizar verificações básicas

- Configurar `ruff` e `mypy` no `pyproject.toml` (ou em pre-commit) para evitar
  regressões de estilo e tipos.
- Adicionar um fluxo de CI simples que execute `uv run pytest` e as checagens de
  lint.

## Como contribuir com a limpeza

1. Verifique esta lista antes de iniciar uma tarefa para evitar duplicidade.
2. Prefira PRs focados (ex.: "Remover módulo legado X"), acompanhados de testes
   quando aplicável.
3. Atualize este documento adicionando um item em "Ações concluídas" após o
   merge.
