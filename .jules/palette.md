# Palette's Journal - UX & Accessibility

## 2024-05-23 - Micro-UX in CLI
**Learning:** Users often run long-running tasks in the CLI without knowing if the process is stuck or progressing. `typer.echo` printing line-by-line is functional but hard to scan.
**Action:** Replace sequential print logs with `rich.progress` bars for window processing to give immediate visual feedback and estimation.

## 2024-10-27 - CLI Output Separation
**Learning:** When adding rich UI elements (like progress bars) to CLI commands that produce structured output (like JSON), it's critical to separate the streams.
**Action:** Always direct interactive UI elements (progress bars, spinners, status updates) to `stderr` while keeping the structured data on `stdout`. This allows users to pipe the output (e.g., `baliza backfill ... | jq`) without the UI characters corrupting the data stream.
## 2025-12-19 - Humanizing CLI Timestamps
**Learning:** Absolute timestamps (e.g., '2025-12-19 15:02') in CLI tables are hard to scan quickly for recent activity. Relative time (e.g., '2h ago') provides immediate context for recent runs, while absolute dates are better for historical records.
**Action:** Use a hybrid approach: display relative time for recent events (< 7 days) and absolute dates for older ones. This reduces cognitive load when checking recent status.
