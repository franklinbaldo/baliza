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
