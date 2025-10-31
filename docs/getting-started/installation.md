# Installation

Baliza provides two installation methods: direct execution with uvx (recommended for most users) and local installation for development.

## Option 1: Direct Execution with uvx (Recommended)

Execute Baliza directly without cloning the repository:

```bash
# Execute directly from GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Usage examples
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza export --table contratos --out data/contratos
```

### Advantages

- ✅ No need to clone the repository
- ✅ Always uses the latest version from `main`
- ✅ Automatic isolated environment
- ✅ Ideal for production and CI/CD

### Creating an Alias

To simplify usage, create an alias (add to your `.bashrc` or `.zshrc`):

```bash
alias baliza='uvx --from "git+https://github.com/franklinbaldo/baliza" baliza'

# Now you can use it simply as:
baliza extract
baliza export --table contratos --out data/contratos
```

## Option 2: Local Installation for Development

Clone the repository and develop locally:

```bash
# Clone repository
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Install dependencies
uv sync

# Execute
uv run baliza extract
uv run baliza export --table contratos --out data/contratos
```

## Requirements

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Internet access to query the public PNCP API

## Optional Dependencies

Baliza has optional dependency groups for different use cases:

```bash
# Install with test dependencies
uv sync --extra test

# Install with development dependencies
uv sync --extra dev

# Install with documentation dependencies
uv sync --extra docs

# Install all optional dependencies
uv sync --all-extras
```

## Verifying Installation

After installation, verify that Baliza is working correctly:

```bash
# Using uvx
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --version

# Using local installation
uv run baliza --version
```

## Next Steps

- [Quick Start Guide](quickstart.md) - Run your first extraction
- [Configuration](configuration.md) - Learn about configuration options
