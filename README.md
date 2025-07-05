# BALIZA

**Acrônimo oficial**
**B**ackup **A**berto de **LI**citações **Z**elando pelo **A**cesso

---

### Pitch de uma linha

> “BALIZA espelha diariamente o PNCP no Internet Archive para garantir histórico completo e análises retroativas de compras públicas.”

### README.md — estrutura mínima (Atualizado)

````markdown
# BALIZA

**Backup Aberto de Licitações Zelando pelo Acesso** — um bot que baixa o delta diário do PNCP e envia para o Internet Archive em JSONL compactado.

## Por quê
- O PNCP só mantém dados acessíveis via API e sem versionamento.
- Sem séries históricas não há auditoria séria nem detecção de fraude por padrão.

## Como funciona
1. **Coleta Agendada**: Diariamente, às 02:15 BRT (05:15 UTC), uma GitHub Action é acionada.
2. **Busca de Dados**: O script `baliza/src/baliza/main.py` chama os endpoints da API do PNCP (inicialmente `/v1/contratacoes/publicacao`, com planos de expansão para outros como `/v1/contratos/publicacao`, `/v1/pca`). Ele filtra os dados para o dia anterior (`dataInicial=dataFinal=ontem`).
3. **Processamento e Compactação**: Os dados coletados são agregados em um arquivo `pncp-<tipo>-YYYY-MM-DD.jsonl.zst` (JSONL compactado com Zstandard).
4. **Upload para o Internet Archive**: O arquivo compactado é enviado para o Internet Archive usando a API S3-like do IA. O identificador do item é no formato `pncp-<tipo>-YYYY-MM-DD`.
5. **Registro de Checksum (Planejado)**: Está planejado salvar o checksum SHA256 dos arquivos processados (ex: em um `state/processed.csv`) para evitar duplicidade e facilitar o rastreamento. Esta funcionalidade ainda não está implementada no script principal.

## Configuração Inicial e Execução

### Pré-requisitos
- Python 3.11+
- `uv` (gerenciador de pacotes Python rápido). Se não tiver, instale via `curl -LsSf https://astral.sh/uv/install.sh | sh`.
- Credenciais do Internet Archive (`IA_KEY` e `IA_SECRET`).

### Variáveis de Ambiente
O script requer as seguintes variáveis de ambiente para o upload no Internet Archive:
- `IA_KEY`: Sua chave de acesso do Internet Archive.
- `IA_SECRET`: Seu segredo do Internet Archive.

Estas variáveis são especialmente importantes para a execução automática via GitHub Actions, onde devem ser configuradas como "Secrets" do repositório.

### Executar Localmente
1. Clone o repositório.
2. Navegue até a raiz do projeto.
3. Crie um ambiente virtual e instale as dependências (se ainda não o fez):
   ```bash
   uv venv  # Cria o .venv
   uv sync  # Instala dependências do uv.lock
   ```
4. Exporte as credenciais do Internet Archive:
   ```bash
   export IA_KEY="SUA_CHAVE_IA"
   export IA_SECRET="SEU_SEGREDO_IA"
   ```
5. Execute o script (substitua `YYYY-MM-DD` pela data desejada):
   ```bash
   uv run python baliza/src/baliza/main.py --date YYYY-MM-DD
   # Alternativamente, a data pode ser fornecida pela variável de ambiente BALIZA_DATE
   # export BALIZA_DATE=YYYY-MM-DD; uv run python baliza/src/baliza/main.py
   ```
   Os arquivos gerados (JSONL, ZST) aparecerão no diretório `baliza_data/` na raiz do projeto.

## Automação com GitHub Actions
- O projeto inclui um workflow em `.github/workflows/baliza_daily_run.yml`.
- Este workflow executa o script diariamente às 02:15 BRT (05:15 UTC), utilizando a variável de ambiente `BALIZA_DATE`.
- O workflow captura um sumário da execução em formato JSON e o armazena como um artefato do GitHub Actions para referência e depuração.
- **Importante**: Para que o upload automático funcione, você **DEVE** configurar `IA_KEY` e `IA_SECRET` como "Secrets" nas configurações do seu repositório GitHub (Settings > Secrets and variables > Actions).

## Roadmap

### Fase 1: Implementação Central (Concluída)
* [x] Script de coleta para `/v1/contratacoes/publicacao` com CLI (`Typer`).
* [x] Compressão dos dados para `.jsonl.zst`.
* [x] Upload dos arquivos para o Internet Archive.
* [x] Agendamento da execução diária via GitHub Actions.
* [x] Captura de sumário estruturado da execução (JSON) como artefato no GitHub Actions.

### Fase 2: Página de Estatísticas e Compartilhamento de Torrents (Planejado)
* [ ] **Coleta de Estatísticas**: Desenvolver script para agregar dados das execuções diárias (e.g., itens coletados, status, links IA).
* [ ] **Manifesto de Torrents**: Gerar e manter uma lista atualizada dos links `.torrent` para os itens arquivados no Internet Archive.
* [ ] **Página Web de Estatísticas**:
    * [ ] Criar template HTML para a página de status.
    * [ ] Desenvolver script para gerar a página HTML estática a partir dos dados de estatísticas e torrents.
    * [ ] Configurar GitHub Pages para hospedar a página.
* [ ] **Atualização do Workflow**: Incrementar o GitHub Actions para executar os scripts de coleta de estatísticas, geração de manifesto de torrents e da página web, e fazer commit dos artefatos atualizados.

### Funcionalidades Adicionais (Consideradas para o Futuro - Pós Fase 2)
* [ ] Implementar coleta para endpoint `/v1/contratos/publicacao`.
* [ ] Implementar coleta para endpoint `/v1/pca` (planejamento).
* [ ] Implementar persistência robusta de checksums e estado de processamento (e.g., `state/processed.csv`) para evitar reprocessamento e duplicatas de forma mais granular.
* [ ] Avaliar a criação de um dump automático para ClickHouse a partir dos dados no Internet Archive.
* [ ] Desenvolver um painel analítico (Superset/Metabase) com KPIs (e.g., sobrepreço) utilizando os dados coletados.
* [ ] Configurar alertas de anomalia (e.g., via Webhook) para falhas na coleta ou problemas nos dados.

## Próximos Passos (Comunidade e Testes)
1. **Teste Manual Extensivo**: Execute o script com `--date` para diferentes dias passados para garantir a robustez do hash, da coleta e do upload.
2. **Feedback e Contribuições**: Abra issues para bugs, sugestões ou melhorias. Contribuições via Pull Requests são bem-vindas!
3. **Anunciar e Engajar**: Após estabilização, considere anunciar no fórum Dados Abertos BR e convidar a comunidade para auditar os dados e o processo.

```

### Projeto em 5 min — visão geral

1. **Use os próprios endpoints de consulta do PNCP** (`/v1/contratacoes/publicacao`, `/v1/contratos/publicacao`, `/v1/pca`, etc.), que já aceitam filtros por intervalo de datas, paginação (`pagina`, `tamanhoPagina ≤ 500`) e devolvem JSON padronizado.&#x20;
2. **Rode um *crawler* diário** (cron ou GitHub Actions) que baixa só o delta do dia anterior. Não invente “varredura completa” — é lento, caro e sujeito a time-out.
3. **Empacote cada lote em `JSONL` comprimido** (`.jsonl.zst` é ótimo) e gere um manifesto SHA-256 para deduplicar depois.
4. **Suba o arquivo para o Internet Archive** usando a API S3-like (`ias3`) com nome estável, ex.:
   `pncp-contratacoes-2025-07-03.jsonl.zst`
   Metadados mínimos: `title`, `creator=“PNCP Mirror Bot”`, `subject=“public procurement Brazil”`. ([archive.org][1], [archive.org][2])
5. **Repita.** Em poucos meses você terá um *data lake* público, versionado e historicamente completo para qualquer análise contábil, *benchmarking* de preços, *red-flag analytics*, etc.

---

### Arquitetura Adotada

| Camada                 | Tech-stack                                     | Por quê                                                      |
| ---------------------- | ---------------------------------------------- | ------------------------------------------------------------ |
| **Coleta**             | `python` + `requests` + `tenacity` (retry)     | Leve, controlado, fácil de debugar                           |
| **Gerenciamento de Dep.** | `uv` (Astral)                                  | Rápido, moderno, compatível com `pyproject.toml`             |
| **Agendamento**        | GitHub Actions (cron)                          | Integrado ao repositório, gratuito para projetos open source   |
| **Processamento**      | Python `json` (para JSONL)                     | Simples e direto para conversão em JSONL                     |
| **Compressão**         | `zstandard` (via lib Python)                   | Excelente taxa de compressão e velocidade                    |
| **Upload**             | `internetarchive` CLI/lib Python               | Biblioteca oficial para interagir com o Internet Archive     |
| **Catálogo (Planejado)** | Manifest (`manifest.yml`) + checksums em CSV   | Garante integridade, evita duplicatas (ainda não implementado) |

---

### Fluxo Incremental (Detalhado)

1. **Cálculo da Data**: O script (ou o workflow do GitHub Actions) determina a data "ontem" (fuso horário de Brasília, UTC-3).
2. **Iteração por Endpoints**: Para cada endpoint configurado (inicialmente, apenas `contratacoes`):
   ```text
   GET https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?dataInicial=YYYY-MM-DD&dataFinal=YYYY-MM-DD&pagina=1&tamanhoPagina=500
   ```
3. **Paginação**: O script itera sobre as páginas de resultados até que `paginaAtual` seja maior ou igual a `totalPaginas` retornado pela API. Cada página pode conter até 500 registros.
4. **Armazenamento Temporário**: Cada registro JSON é anexado a um arquivo `.jsonl` local.
5. **Compressão**: Após coletar todos os dados do dia para um endpoint, o arquivo `.jsonl` é comprimido usando Zstandard, resultando em um arquivo `.jsonl.zst`.
6. **Cálculo de Checksum**: Um hash SHA256 é calculado para o arquivo `.jsonl.zst`.
7. **Upload para Internet Archive**: O arquivo comprimido é enviado para o Internet Archive, e o checksum SHA256 é incluído nos metadados.
8. **Limpeza (Local)**: O arquivo `.jsonl` original é removido após a compressão e tentativa de upload. O arquivo `.jsonl.zst` permanece localmente no diretório `baliza_data/`.
9. **Registro de Estado (Planejado)**: Futuramente, o hash do arquivo e o status do upload serão gravados para evitar reprocessamento e permitir o rastreamento.

---

### Pontos críticos (opinião sem rodeios)

* **Rate-limit e disponibilidade**: Se o *crawler* falhar, não re-tente infinito — o PNCP derruba conexões longas.
* **Token de acesso**: Hoje a consulta é pública, mas o SERPRO pode exigir API-key amanhã; prepare var env.
* **Qualidade dos dados**: Campos financeiros vêm como texto, vírgula decimal e zeros mágicos (0 = sigilo). Não confie neles sem *post-processing*.&#x20;
* **Internet Archive não é banco OLTP**: ele armazena blob; para consultas SQL use BigQuery, ClickHouse ou DuckDB apontando para seus `JSONL`.
* **Legalidade**: dados já são públicos; o espelho é mera redundância. Mas inclua aviso de responsabilidade (“*dados brutos, sem garantias*”).

---

### Próximos passos (Pós-Implementação Inicial)

1. **Repositório e Licença**: O repositório no GitHub está configurado com Licença MIT e este README atualizado. (Feito!)
2. **Automação**: GitHub Actions está habilitado com `cron: '15 5 * * *'` (02:15 BRT / 05:15 UTC) para execução diária. (Feito!)
3. **Dashboard (Futuro)**: Criar um *dashboard* (ex: Superset, Metabase) que consuma os dados dos arquivos `.jsonl.zst` diretamente do Internet Archive (possivelmente via HTTPFS ou similar).
4. **Engajamento Comunitário (Futuro)**: Quando o sistema estiver estável e com um volume razoável de dados arquivados, anunciar no fórum **Dados Abertos BR** para atrair colaboradores, auditores e usuários.

Com a base implementada, o projeto Baliza está pronto para começar a arquivar os dados e evoluir com as funcionalidades planejadas no Roadmap.

[1]: https://archive.org/developers/ias3.html?utm_source=chatgpt.com "ias3 Internet archive S3-like API"
[2]: https://archive.org/developers/index-apis.html?utm_source=chatgpt.com "Tools and APIs — Internet Archive Developer Portal"
