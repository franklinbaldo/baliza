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

## 2025-12-20 - Helpful Empty States
**Learning:** Empty states in CLI tools often look like errors or silent failures.
**Action:** Use `rich.Panel` to wrap empty state messages. Include a clear explanation of *why* it's empty and a specific call-to-action (command to run) to fix it. This turns a dead end into a guidepost.

## 2025-02-18 - Visualizing Distribution in Tables
**Learning:** Percentage columns in CLI tables are informative but hard to scan for "at-a-glance" status.
**Action:** Embed `rich.progress_bar.ProgressBar` directly into table cells to create visual distribution bars. Use distinct colors (green/yellow/red) to reinforce the status meaning visually.

---

## 2024-05-22 - [CLI Output Delights]
**Learning:** CLI users appreciate concise performance metrics (duration) and readable numbers (thousands separators) just as much as GUI users.
**Action:** Always include duration and structured summaries for long-running CLI tasks.

## 2024-05-23 - [Visual Loading States]
**Learning:** Replacing plain text loading messages with visual indicators (spinners) and pulsing text significantly improves perceived performance and polish.
**Action:** Use CSS-only spinners (Tailwind `animate-spin`) combined with `role="status"` for accessible and delightful loading states.

## 2024-05-24 - [Accessible Data Visualizations]
**Learning:** Dense data grids (heatmaps) are invisible to screen readers without a summary. Adding `role="img"` and a calculated `aria-label` provides a "glanceable" equivalent for AT users.
**Action:** Always provide a text summary for complex visualizations, not just individual data points.

## 2024-05-25 - [Keyboard Shortcuts]
**Learning:** Adding keyboard shortcuts (like 'R' for refresh) transforms a passive dashboard into a power-user tool.
**Action:** Pair every main action button with a documented keyboard shortcut and a tooltip hint.

## 2024-06-05 - [Tab Personality & Focus]
**Learning:** A custom favicon (even a simple emoji SVG) makes a tab instantly recognizable, and clear focus styles on secondary links (like footers) turn "hidden" navigation into accessible paths.
**Action:** Always add a data-URI favicon to standalone HTML tools and ensure all interactive elements, even minor ones, have visible focus states.

## 2024-06-13 - [Localized Status Labels]
**Learning:** Localizing status codes in UI components and tooltips significantly reduces cognitive load for non-English users, especially when technical terms (like 'uploaded') are mapped to user-friendly labels ('Enviado').
**Action:** Always map internal status codes to localized, human-readable labels in the UI layer, rather than displaying raw database values.

## 2026-02-11 - [Actionable Error States]
**Learning:** A blank "Error" text is a dead end. Providing a clear "Try Again" button not only empowers the user but also turns a frustration point into a recoverable flow. Also, when using React via CDN with Lucide, explicit `key` props are crucial to prevent DOM conflicts during unmounting/remounting of icon-heavy components.
**Action:** Always include a recovery action in error states and ensure component identity is managed explicitly when manipulating the DOM with external libraries.
