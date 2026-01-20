# Baliza CLI

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) é uma **ferramenta
de linha de comando** de código aberto que captura dados de contratos do Portal
Nacional de Contratações Públicas (PNCP) e os armazena em um banco **DuckDB**
pronto para análise. O projeto nasceu para preservar o histórico das compras
públicas brasileiras e oferecer uma base consistente para jornalistas,
pesquisadores e órgãos de controle.

> **⚠️ Este repositório contém apenas o CLI de extração de dados.**
> Para visualização, dashboards e interface web, veja o projeto `baliza-site`
> (em breve). Documentação completa da arquitetura em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Visão geral

- **Extrator Resiliente com DuckDB:** O Baliza utiliza um pipeline robusto construído com `httpx` para chamadas de API e `DuckDB` para armazenamento de dados e estado. Ele foi projetado para ser resiliente a falhas e totalmente resumível.
- **CLI Inteligente:** O comando `baliza extract` opera em modo automático por padrão. Ele consulta seu próprio banco de dados de estado (`baliza_state.coverage` no DuckDB) para encontrar a última data extraída com sucesso e continua o trabalho a partir daí, garantindo que nenhuma janela de dados seja perdida.
- **Fluxo Bronze → Parquet:** `baliza extract` salva os dados brutos no DuckDB (`baliza.duckdb`). O comando `baliza export` pode ser usado para gerar arquivos Parquet para análise e arquivamento de longo prazo.
- **Transparência e Estado:** Cada janela diária processada é registrada na tabela `baliza_state.coverage`, armazenando o status (`complete` ou `failed`), o número de páginas e registros extraídos. O comando `baliza verify` pode ser usado para auditar a cobertura e encontrar lacunas.
- **Documentação de Arquitetura:** As decisões de design e os planos de evolução do pipeline são mantidos no diretório `docs/`.

> 📌 **Escopo atual:** O pipeline atualmente foca no endpoint de **contratos**. A estratégia para incluir outros recursos do PNCP está detalhada nos documentos de arquitetura.

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

Execute o Baliza sem precisar clonar o repositório:

```bash
# Executar diretamente do GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Exemplos de uso
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza export --table contratos --out data/contratos
```

**Vantagens:**
- ✅ Não precisa clonar o repositório
- ✅ Sempre usa a versão mais recente do `main`
- ✅ Ambiente isolado automaticamente
- ✅ Ideal para uso em produção e CI/CD

### Opção 2: Instalação local para desenvolvimento

Clone o repositório e desenvolva localmente:

```bash
# Clonar repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Instalar dependências
uv sync

# Executar
uv run baliza extract
uv run baliza export --table contratos --out data/contratos
```

### Opção 3: Docker (Produção e CI/CD)

Execute o Baliza em um container isolado:

```bash
# Baixar imagem do GitHub Container Registry
docker pull ghcr.io/franklinbaldo/baliza:latest

# Verificar versão
docker run --rm ghcr.io/franklinbaldo/baliza:latest --version

# Extrair dados (com volume montado)
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest extract

# Exportar para Parquet
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest export --table contratos --out /data/contratos

# Backfill
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest backfill 2024-01 2024-03
```

**Vantagens:**
- ✅ Ambiente completamente isolado
- ✅ Sem necessidade de instalar Python ou uv
- ✅ Ideal para produção e CI/CD
- ✅ Reprodutível em qualquer sistema com Docker

> **Nota:** O arquivo `baliza.duckdb` será criado no diretório montado (`-v $(pwd)/data:/data`).
> Certifique-se de montar um volume para persistir o estado entre execuções.

### Requisitos

**Para uvx ou instalação local:**
- Python 3.11 ou superior
- [uv](https://github.com/astral-sh/uv) instalado (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Acesso à internet para consultar a API pública do PNCP

**Para Docker:**
- Docker instalado ([Guia de instalação](https://docs.docker.com/get-docker/))
- ~500MB de espaço em disco para imagem
- Acesso à internet para download da imagem e consulta à API

## Início rápido

### Usando uvx (sem clonar)

```bash
# Alias para simplificar (adicione ao seu .bashrc ou .zshrc)
alias baliza='uvx --from "git+https://github.com/franklinbaldo/baliza" baliza'

# Executar a extração automática (recomendado)
# O Baliza encontrará a última data salva e continuará de onde parou.
baliza extract

# Extrair um período específico (modo manual)
baliza extract --start 2024-01-01 --end 2024-01-31

# Exportar para Parquet
baliza export --table contratos --output data/contratos

# Verificar cobertura
baliza verify --start 2024-01-01 --end 2024-01-31
```

### Usando instalação local

```bash
# Dentro do diretório do projeto
# Executar a extração automática
uv run baliza extract

# Exportar para Parquet
uv run baliza export --table contratos --output data/contratos
```

O comando `baliza extract` cria (ou atualiza) o arquivo `baliza.duckdb` no
diretório atual. Por padrão, ele é executado em **modo automático**, consultando o
estado interno para determinar o intervalo de datas a ser processado. Se uma
extração falhar, basta executá-lo novamente para que ele continue do último dia
bem-sucedido.

## Inspecionando os dados

Abra o DuckDB gerado diretamente pelo shell:

```bash
uv run python -m duckdb --batch <<'SQL'
.open baliza.duckdb
USE baliza_raw;
SELECT COUNT(*) AS total_contratos,
       MAX(dataatualizacao) AS ultima_atualizacao
FROM contratos;
SQL
```

Também é possível utilizar pandas ou polars:

```python
import duckdb

con = duckdb.connect("baliza.duckdb")
con.execute("USE baliza_raw")
contratos = con.execute("SELECT * FROM contratos").df()
print(contratos.head())
```

## Comandos disponíveis

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Executa o pipeline de extração. Por padrão, opera em modo automático e resumível. |
| `baliza export` | Exporta uma tabela do DuckDB para um arquivo Parquet. |
| `baliza verify` | Audita a cobertura de dados em um intervalo de datas e reporta lacunas. |

**Opções úteis para `extract`:**

- `--start <AAAA-MM-DD> --end <AAAA-MM-DD>`: Ativa o modo manual para extrair um intervalo específico.
- `--lookback-days N`: No modo automático, define quantos dias retroceder a partir da última data bem-sucedida para buscar atualizações (padrão: 3).
- `--duckdb /caminho/arquivo.duckdb`: Define um caminho customizado para o arquivo DuckDB.

Use `uv run baliza --help` para ver todos os parâmetros suportados.

### Extração Resumível (Resumable Extraction) ✨

O Baliza possui uma extração **resumível e baseada em estado**, tornando o pipeline robusto e confiável.

**Como funciona:**

1.  **Estado no DuckDB:** O Baliza rastreia cada janela diária de extração na tabela `baliza_state.coverage` dentro do arquivo `baliza.duckdb`. Cada registro armazena a data, o status (`complete` ou `failed`), e metadados da execução.

2.  **Inicialização Automática:** Ao executar `baliza extract` sem especificar `--start` ou `--end`, o pipeline consulta essa tabela para encontrar a data mais recente que foi concluída com sucesso.

3.  **Retomada Inteligente:** A extração começa a partir do dia seguinte ao último sucesso. Se a última execução falhou no meio do caminho, o Baliza a retomará automaticamente, reprocessando apenas os dias que faltam. Um período de *lookback* (configurável com `--lookback-days`) garante que dados atualizados recentemente também sejam capturados.

**Exemplo de uso:**

```bash
# Primeira execução: O Baliza começa do início do projeto (ex: 2023-01-01)
$ uv run baliza extract
Starting automatic extraction...
No previous run found. Starting from project start: 2023-01-01
...
Processing 2023-01-15...
✗ Extraction failed: Connection timeout

# O processo parou. Basta rodar o comando novamente para continuar.
$ uv run baliza extract
Starting automatic extraction...
Found last completed run. Starting from 2023-01-12 (last success on 2023-01-14 with 3-day lookback).
...
Processing 2023-01-12...
✓ Completed
...
✓ Automatic extraction complete!
```

**Benefícios:**

- ✅ **Confiabilidade:** Recupera-se automaticamente de falhas de rede ou da API.
- ✅ **Eficiência:** Não refaz o trabalho já concluído, economizando tempo e chamadas de API.
- ✅ **Simplicidade:** A complexidade do estado é gerenciada automaticamente. O usuário só precisa executar o mesmo comando simples.

## Arquitetura do Ecossistema

O projeto Baliza é dividido em **dois repositórios independentes**:

| Repositório | Responsabilidade | Status |
|------------|------------------|--------|
| **baliza** (este) | CLI de extração, transformação e exportação de dados | ✅ Ativo |
| **baliza-site** | Interface web, visualização e consultas | 🔜 Em breve |

Veja [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) para detalhes completos.

## Estrutura deste repositório

```
├── src/baliza/
│   ├── cli.py              # Interface de linha de comando (Typer)
│   └── extractor.py        # Lógica de extração e controle de estado
├── docs/                   # Guias de arquitetura e planos de evolução
├── tests/                  # Testes automatizados
└── pyproject.toml          # Metadados e dependências do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).
