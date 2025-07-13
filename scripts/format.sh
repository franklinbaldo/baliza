#!/bin/bash
# Baliza Code Formatter
# Quick formatting script using Ruff

set -e

echo "🎨 BALIZA CODE FORMATTER"
echo "========================"

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo "❌ Error: Run this script from the project root directory"
    exit 1
fi

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "✅ Activated virtual environment"
else
    echo "⚠️ Virtual environment not found, using system Python"
fi

# Format Python code with Ruff
echo ""
echo "🔧 Formatting Python code with Ruff..."
uv run ruff format .

# Fix import sorting
echo ""
echo "📦 Fixing import sorting..."
uv run ruff check --select I --fix .

# Fix other auto-fixable issues
echo ""
echo "🔨 Fixing auto-fixable linting issues..."
uv run ruff check --fix .

# Format SQL files if they exist
if command -v sqlfluff &> /dev/null && find . -name "*.sql" -type f | head -1 > /dev/null; then
    echo ""
    echo "🗃️ Formatting SQL files..."
    find . -name "*.sql" -type f -exec sqlfluff fix {} \; || echo "⚠️ SQL formatting skipped (some errors)"
fi

# Format YAML files if prettier is available
if command -v prettier &> /dev/null; then
    echo ""
    echo "📋 Formatting YAML files..."
    prettier --write "**/*.{yml,yaml}" || echo "⚠️ YAML formatting skipped"
fi

echo ""
echo "✅ Code formatting completed!"
echo ""
echo "💡 Next steps:"
echo "  - Review the changes with: git diff"
echo "  - Run quality checks with: python scripts/lint.py"
echo "  - Run tests with: uv run pytest"