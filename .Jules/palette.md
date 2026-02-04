## 2024-05-22 - [CLI Output Delights]
**Learning:** CLI users appreciate concise performance metrics (duration) and readable numbers (thousands separators) just as much as GUI users.
**Action:** Always include duration and structured summaries for long-running CLI tasks.

## 2026-02-03 - [ARIA Progress Bar Pattern]
**Learning:** For custom-built progress bars (divs), adding `role="progressbar"` and dynamic `aria-valuenow` is essential for screen readers, and `aria-hidden="true"` on the text percentage prevents double announcement.
**Action:** Always implement full ARIA attributes when creating custom progress components, not just visual width.

## 2026-02-03 - [Obsolete PR]
**Learning:** This PR was marked as obsolete by the maintainer as the functionality exists in newer PRs.
**Action:** Close work on this branch.
