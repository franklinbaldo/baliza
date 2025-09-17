# Plano de limpeza do repositório

Este documento acompanha o esforço de redução de legado e simplificação do
Baliza após a migração para o pipeline declarativo com dlt. O objetivo é manter
apenas os componentes necessários para extrair dados do PNCP e entregá-los em
formato analítico.

## Estado atual

- O pipeline declarativo (`src/baliza/pipelines/pncp.py`) já orquestra a
  extração com dlt e grava os dados no DuckDB local.
- O pacote `baliza.extraction` ainda contém utilitários experimentais e código
  parcialmente migrado que não é utilizado pela CLI atual.
- Há diretórios herdados (`pipelines/`, `extraction/`, `utils/`) com funções
  sobrepostas, além de arquivos de configuração duplicados.
- A suíte de testes end-to-end foi removida durante a migração e precisa ser
  recriada.

## Ações concluídas ✅

- Criação da configuração declarativa `config/pncp.yml` para o endpoint de
  contratos.
- Substituição do fluxo Prefect pelo comando `baliza extract` baseado em dlt.
- Documentação do fluxo incremental e dos parâmetros de execução na nova
  `README.md`.

## Próximos passos

### 1. Consolidar módulos de extração

- Remover funções não utilizadas em `baliza.extraction.*` ou migrá-las para
  `baliza.pipelines` quando ainda forem necessárias.
- Eliminar arquivos de backup (`settings.py.backup`) e referências ao antigo
  layout.
- Revisar imports para garantir que toda a CLI dependa apenas dos módulos
  consolidados.

### 2. Atualizar e expandir testes

- Recriar um teste end-to-end mínimo que execute `run_pncp` com um mock do
  endpoint e valide a escrita no DuckDB.
- Adicionar testes unitários para `_apply_incremental_overrides` e outras
  funções utilitárias críticas.

### 3. Remover documentação obsoleta

- Substituir referências à arquitetura antiga (Prefect, múltiplos comandos)
  pelos planos descritos nos documentos atualizados em `docs/`.
- Garantir que novos PRs atualizem sempre README e documentos técnicos quando
  introduzirem funcionalidades.

### 4. Automatizar verificações básicas

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
