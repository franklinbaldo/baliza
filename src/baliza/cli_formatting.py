"""CLI formatting and Rich console utilities."""

from rich.console import Console

# Global Rich console instance
console = Console()

# Gap visualization constants
GAP_ICONS = {
    "incomplete": "🚧",
    "suspect": "🚩",
    "missing": "📥",
    "unprocessed": "🆕",
    "lookback": "🔙",
}

GAP_COLORS = {
    "incomplete": "yellow",
    "suspect": "red",
    "missing": "blue",
    "unprocessed": "cyan",
    "lookback": "magenta",
}
