# Pncp CLI

API REST de serviços do Portal Nacional de Contratações Públicas (PNCP)

Learn more at [Pncp](https://www.serpro.gov.br).

## Install

The recommended path installs both the `pncp-pp-cli` binary and the `pp-pncp` agent skill in one shot:

```bash
npx -y @mvanhorn/printing-press install pncp
```

For CLI only (no skill):

```bash
npx -y @mvanhorn/printing-press install pncp --cli-only
```


### Without Node

The generated install path is category-agnostic until this CLI is published. If `npx` is not available before publish, install Node or use the category-specific Go fallback from the public-library entry after publish.

### Pre-built binary

Download a pre-built binary for your platform from the [latest release](https://github.com/mvanhorn/printing-press-library/releases/tag/pncp-current). On macOS, clear the Gatekeeper quarantine: `xattr -d com.apple.quarantine <binary>`. On Unix, mark it executable: `chmod +x <binary>`.

<!-- pp-hermes-install-anchor -->
## Install for Hermes

From the Hermes CLI:

```bash
hermes skills install mvanhorn/printing-press-library/cli-skills/pp-pncp --force
```

Inside a Hermes chat session:

```bash
/skills install mvanhorn/printing-press-library/cli-skills/pp-pncp --force
```

## Install for OpenClaw

Tell your OpenClaw agent (copy this):

```
Install the pp-pncp skill from https://github.com/mvanhorn/printing-press-library/tree/main/cli-skills/pp-pncp. The skill defines how its required CLI can be installed.
```

## Quick Start

### 1. Install

See [Install](#install) above.

### 2. Set Up Credentials

Get your access token from your API provider's developer portal, then store it:

```bash
pncp-pp-cli auth set-token YOUR_TOKEN_HERE
```

Or set it via environment variable:

```bash
export PNCP_CONSULTA_BEARER_AUTH="your-token-here"
```

### 3. Verify Setup

```bash
pncp-pp-cli doctor
```

This checks your configuration and credentials.

### 4. Try Your First Command

```bash
pncp-pp-cli atas consultar-registro-preco-data-atualizacao --data-inicial example-value --data-final example-value
```

## Usage

Run `pncp-pp-cli --help` for the full command reference and flag list.

## Commands

### atas

Consultas de Atas de Registro de Preços

- **`pncp-pp-cli atas consultar-registro-preco-data-atualizacao`** - Consultar Atas de Registro de Preço por Data de Atualização Global
- **`pncp-pp-cli atas consultar-registro-preco-periodo`** - Consultar Ata de Registro de Preço por Período de Vigência

### contratacoes

Manage contratacoes

- **`pncp-pp-cli contratacoes consultar-contratacao-periodo-recebimento-propostas`** - Consultar Contratações com Recebimento de Propostas Aberto
- **`pncp-pp-cli contratacoes consultar-contratacao-por-data-de-publicacao`** - Consultar Contratações por Data de Publicação
- **`pncp-pp-cli contratacoes consultar-contratacao-por-data-ultima-atualizacao`** - Consultar Contratações por Data de Atualização Global

### contratos

Manage contratos

- **`pncp-pp-cli contratos consultar-por-data-atualizacao-global`** - Consultar Contratos/Empenhos por Data de Atualização Global
- **`pncp-pp-cli contratos consultar-por-data-publicacao`** - Consultar Contratos por Data de Publicação

### instrumentoscobranca

Manage instrumentoscobranca

- **`pncp-pp-cli instrumentoscobranca consultar-instrumentos`** - Consultar Instrumentos de Cobrança por Data de Inclusão

### orgaos

Manage orgaos


### pca

Manage pca

- **`pncp-pp-cli pca consultar-itens-por-ano`** - Consultar Itens de PCA por Ano do PCA e Código de Classificação Superior
- **`pncp-pp-cli pca consultar-itens-por-usuario-ano`** - Consultar Itens de PCA por Ano do PCA, IdUsuario e Código de Classificação Superior
- **`pncp-pp-cli pca consultar-itens-por-usuario-ano-1`** - Consultar PCA por Data de Atualização Global


## Output Formats

```bash
# Human-readable table (default in terminal, JSON when piped)
pncp-pp-cli atas consultar-registro-preco-data-atualizacao --data-inicial example-value --data-final example-value

# JSON for scripting and agents
pncp-pp-cli atas consultar-registro-preco-data-atualizacao --data-inicial example-value --data-final example-value --json

# Filter to specific fields
pncp-pp-cli atas consultar-registro-preco-data-atualizacao --data-inicial example-value --data-final example-value --json --select id,name,status

# Dry run — show the request without sending
pncp-pp-cli atas consultar-registro-preco-data-atualizacao --data-inicial example-value --data-final example-value --dry-run

# Agent mode — JSON + compact + no prompts in one flag
pncp-pp-cli atas consultar-registro-preco-data-atualizacao --data-inicial example-value --data-final example-value --agent
```

## Agent Usage

This CLI is designed for AI agent consumption:

- **Non-interactive** - never prompts, every input is a flag
- **Pipeable** - `--json` output to stdout, errors to stderr
- **Filterable** - `--select id,name` returns only fields you need
- **Previewable** - `--dry-run` shows the request without sending
- **Read-only by default** - this CLI does not create, update, delete, publish, send, or mutate remote resources
- **Offline-friendly** - sync/search commands can use the local SQLite store when available
- **Agent-safe by default** - no colors or formatting unless `--human-friendly` is set

Exit codes: `0` success, `2` usage error, `3` not found, `4` auth error, `5` API error, `7` rate limited, `10` config error.

## Use with Claude Code

Install the focused skill — it auto-installs the CLI on first invocation:

```bash
npx skills add mvanhorn/printing-press-library/cli-skills/pp-pncp -g
```

Then invoke `/pp-pncp <query>` in Claude Code. The skill is the most efficient path — Claude Code drives the CLI directly without an MCP server in the middle.

<details>
<summary>Use as an MCP server in Claude Code (advanced)</summary>

If you'd rather register this CLI as an MCP server in Claude Code, install the MCP binary first:


Install the MCP binary from this CLI's published public-library entry or pre-built release.

Then register it:

```bash
claude mcp add pncp pncp-pp-mcp -e PNCP_CONSULTA_BEARER_AUTH=<your-token>
```

</details>

## Use with Claude Desktop

This CLI ships an [MCPB](https://github.com/modelcontextprotocol/mcpb) bundle — Claude Desktop's standard format for one-click MCP extension installs (no JSON config required).

To install:

1. Download the `.mcpb` for your platform from the [latest release](https://github.com/mvanhorn/printing-press-library/releases/tag/pncp-current).
2. Double-click the `.mcpb` file. Claude Desktop opens and walks you through the install.
3. Fill in `PNCP_CONSULTA_BEARER_AUTH` when Claude Desktop prompts you.

Requires Claude Desktop 1.0.0 or later. Pre-built bundles ship for macOS Apple Silicon (`darwin-arm64`) and Windows (`amd64`, `arm64`); for other platforms, use the manual config below.

<details>
<summary>Manual JSON config (advanced)</summary>

If you can't use the MCPB bundle (older Claude Desktop, unsupported platform), install the MCP binary and configure it manually.


Install the MCP binary from this CLI's published public-library entry or pre-built release.

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "pncp": {
      "command": "pncp-pp-mcp",
      "env": {
        "PNCP_CONSULTA_BEARER_AUTH": "<your-key>"
      }
    }
  }
}
```

</details>

## Health Check

```bash
pncp-pp-cli doctor
```

Verifies configuration, credentials, and connectivity to the API.

## Configuration

Config file: `~/.config/pncp-consulta-pp-cli/config.toml`

Static request headers can be configured under `headers`; per-command header overrides take precedence.

Environment variables:

| Name | Kind | Required | Description |
| --- | --- | --- | --- |
| `PNCP_CONSULTA_BEARER_AUTH` | per_call | Yes | Set to your API credential. |

## Troubleshooting
**Authentication errors (exit code 4)**
- Run `pncp-pp-cli doctor` to check credentials
- Verify the environment variable is set: `echo $PNCP_CONSULTA_BEARER_AUTH`
**Not found errors (exit code 3)**
- Check the resource ID is correct
- Run the `list` command to see available items

---

Generated by [CLI Printing Press](https://github.com/mvanhorn/cli-printing-press)
