## 2024-05-22 - [CLI Output Delights]
**Learning:** CLI users appreciate concise performance metrics (duration) and readable numbers (thousands separators) just as much as GUI users.
**Action:** Always include duration and structured summaries for long-running CLI tasks.

## 2024-05-23 - [Visual Loading States]
**Learning:** Replacing plain text loading messages with visual indicators (spinners) and pulsing text significantly improves perceived performance and polish.
**Action:** Use CSS-only spinners (Tailwind `animate-spin`) combined with `role="status"` for accessible and delightful loading states.

## 2024-10-25 - [React CDN + Icons]
**Learning:** When using React and Lucide via CDN, `lucide.createIcons()` can clash with React's virtual DOM updates during state changes (like loading spinners).
**Action:** Use inline SVGs for dynamic, state-dependent icons (like spinners) to avoid re-render conflicts and ensure reliable animation.
