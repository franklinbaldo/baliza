"""Strip style/css from HTML, Astro, and Svelte files.

Removes:
- <style>...</style> blocks (including scoped, lang="...", etc.)
- inline style="..." / style='...' / style={...} attributes
- class="..." / class='...' / class={...} attributes and Svelte class:foo directives
- <link rel="stylesheet" ...> tags
- CSS file imports: import './x.css' / import 'x.scss' / @import url(...)

Usage: python scripts/strip_styles.py [root]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

EXTENSIONS = {".html", ".astro", ".svelte"}
SKIP_DIRS = {"node_modules", ".git", "dist", "build", ".next", ".astro", ".svelte-kit", "__pycache__"}

STYLE_BLOCK_RE = re.compile(r"<style\b[^>]*>.*?</style>", re.DOTALL | re.IGNORECASE)
LINK_STYLESHEET_RE = re.compile(
    r"""<link\b[^>]*\brel\s*=\s*["']stylesheet["'][^>]*/?>""",
    re.IGNORECASE,
)
CSS_ES_IMPORT_RE = re.compile(
    r"""^[ \t]*import\s+(?:[^'";]+\s+from\s+)?["'][^"']+\.(?:css|scss|sass|less|styl|pcss)["']\s*;?[ \t]*\r?\n?""",
    re.MULTILINE,
)
CSS_AT_IMPORT_RE = re.compile(r"""@import\s+(?:url\()?["'][^"']+["']\)?[^;]*;""", re.IGNORECASE)


def _strip_balanced_attr(text: str, attr_name: str) -> str:
    """Remove attr="..."/'...'/{...} occurrences, handling nested braces in JSX/Astro/Svelte."""
    pattern = re.compile(rf"""\s+{attr_name}\s*=\s*""", re.IGNORECASE)
    out = []
    i = 0
    while True:
        m = pattern.search(text, i)
        if not m:
            out.append(text[i:])
            break
        out.append(text[i : m.start()])
        j = m.end()
        if j >= len(text):
            break
        ch = text[j]
        if ch in ("'", '"'):
            end = text.find(ch, j + 1)
            if end == -1:
                out.append(text[m.start():])
                break
            i = end + 1
        elif ch == "{":
            depth = 1
            k = j + 1
            while k < len(text) and depth > 0:
                c = text[k]
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                k += 1
            i = k
        else:
            # bare value (rare in HTML); consume until whitespace or >
            k = j
            while k < len(text) and text[k] not in (" ", "\t", "\n", "\r", ">", "/"):
                k += 1
            i = k
    return "".join(out)


def _strip_svelte_class_directives(text: str) -> str:
    # class:foo / class:foo={expr} / class:foo="bar"
    pattern = re.compile(r"""\s+class:[A-Za-z_][\w-]*""")
    out = []
    i = 0
    while True:
        m = pattern.search(text, i)
        if not m:
            out.append(text[i:])
            break
        out.append(text[i : m.start()])
        j = m.end()
        if j < len(text) and text[j] == "=":
            j += 1
            if j < len(text) and text[j] in ("'", '"'):
                quote = text[j]
                end = text.find(quote, j + 1)
                if end == -1:
                    i = len(text)
                    continue
                i = end + 1
            elif j < len(text) and text[j] == "{":
                depth = 1
                k = j + 1
                while k < len(text) and depth > 0:
                    c = text[k]
                    if c == "{":
                        depth += 1
                    elif c == "}":
                        depth -= 1
                    k += 1
                i = k
            else:
                i = j
        else:
            i = j
    return "".join(out)


def _collapse_blank_lines(text: str) -> str:
    return re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", text)


def strip(text: str) -> str:
    text = STYLE_BLOCK_RE.sub("", text)
    text = LINK_STYLESHEET_RE.sub("", text)
    text = CSS_ES_IMPORT_RE.sub("", text)
    text = CSS_AT_IMPORT_RE.sub("", text)
    text = _strip_balanced_attr(text, "style")
    text = _strip_balanced_attr(text, "class")
    text = _strip_balanced_attr(text, "className")
    text = _strip_svelte_class_directives(text)
    text = _collapse_blank_lines(text)
    return text


def iter_files(root: Path):
    for p in root.rglob("*"):
        if not p.is_file() or p.suffix not in EXTENSIONS:
            continue
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        yield p


def main(argv: list[str]) -> int:
    root = Path(argv[1]) if len(argv) > 1 else Path.cwd()
    changed = 0
    total = 0
    for path in iter_files(root):
        total += 1
        original = path.read_text(encoding="utf-8")
        updated = strip(original)
        if updated != original:
            path.write_text(updated, encoding="utf-8")
            changed += 1
            print(f"stripped: {path}")
    print(f"\n{changed}/{total} files modified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
