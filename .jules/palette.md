# Palette's Journal - UX & Accessibility

## 2024-05-23 - Micro-UX in CLI
**Learning:** Users often run long-running tasks in the CLI without knowing if the process is stuck or progressing. `typer.echo` printing line-by-line is functional but hard to scan.
**Action:** Replace sequential print logs with `rich.progress` bars for window processing to give immediate visual feedback and estimation.
