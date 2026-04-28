#!/bin/bash
set -euo pipefail

# Only run in remote (Claude Code on the web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

echo '{"async": true, "asyncTimeout": 300000}'

REPO="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"

# Python deps (uv — installs into cached venv)
echo "[session-start] Installing Python dependencies..."
cd "$REPO"
uv sync --extra test

# Node deps for web/
echo "[session-start] Installing Node.js dependencies..."
cd "$REPO/web"
npm install
