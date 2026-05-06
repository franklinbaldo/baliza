"""Extract class names that were removed by strip_styles.py.

Reads each HTML/Astro/Svelte file's pre-strip version from git (HEAD~1) and
collects every class name found in class="..." / class='...' / class={...}
attributes and Svelte class:foo directives. Writes a CSV: file_path, classes.

Usage: python scripts/extract_removed_classes.py [--ref HEAD~1] [--out classes_removed.csv]
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
from pathlib import Path

EXTENSIONS = {".html", ".astro", ".svelte"}
SKIP_DIRS = {"node_modules", ".git", "dist", "build", ".next", ".astro", ".svelte-kit", "__pycache__"}

CLASS_ATTR_RE = re.compile(
    r"""\bclass(?:Name)?\s*=\s*(?P<val>"[^"]*"|'[^']*'|\{[^}]*\})""",
    re.IGNORECASE,
)
CLASS_DIRECTIVE_RE = re.compile(r"""\bclass:([A-Za-z_][\w-]*)""")
# Identifier-ish tokens inside a class string; ignores variable interpolations.
TOKEN_RE = re.compile(r"""[A-Za-z_][\w-]*""")


def list_tracked_files(ref: str) -> list[Path]:
    out = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", ref], text=True
    )
    files: list[Path] = []
    for line in out.splitlines():
        p = Path(line)
        if p.suffix not in EXTENSIONS:
            continue
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        files.append(p)
    return files


def show_at(ref: str, path: Path) -> str | None:
    try:
        return subprocess.check_output(
            ["git", "show", f"{ref}:{path.as_posix()}"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return None


def extract_classes(text: str) -> list[str]:
    seen: dict[str, None] = {}

    for m in CLASS_ATTR_RE.finditer(text):
        raw = m.group("val")
        inner = raw[1:-1]
        if raw.startswith("{"):
            # JSX/Astro/Svelte expression; pull bare string literals out.
            for s in re.findall(r"""["']([^"']+)["']""", inner):
                for tok in TOKEN_RE.findall(s):
                    seen.setdefault(tok)
        else:
            # Mustache/handlebars-style {expr} interpolation -> drop those segments.
            cleaned = re.sub(r"\{[^}]*\}", " ", inner)
            for tok in TOKEN_RE.findall(cleaned):
                seen.setdefault(tok)

    for m in CLASS_DIRECTIVE_RE.finditer(text):
        seen.setdefault(m.group(1))

    return list(seen.keys())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="HEAD~1", help="git ref with pre-strip files")
    ap.add_argument("--out", default="classes_removed.csv")
    args = ap.parse_args()

    rows: list[tuple[str, str]] = []
    for path in list_tracked_files(args.ref):
        original = show_at(args.ref, path)
        if original is None:
            continue
        classes = extract_classes(original)
        if not classes:
            continue
        rows.append((path.as_posix(), " ".join(classes)))

    rows.sort(key=lambda r: r[0])

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["file", "classes"])
        w.writerows(rows)

    print(f"wrote {len(rows)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
