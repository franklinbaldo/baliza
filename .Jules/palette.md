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
