# Palette's UX Journal

## 2024-05-22 - CLI Feedback Patterns
**Learning:** CLI commands performing database operations (like `COPY` or `INSERT`) often lack visual feedback, making the app appear frozen. `rich.console.Status` is a simple, high-impact fix.
**Action:** Audit all long-running CLI commands for blocking IO and wrap them in status spinners.
