# Baliza Dashboard Specifications

## 1. Executive Summary
The **Baliza Dashboard** is the primary monitoring interface for the PNCP extraction pipeline. Its purpose is to provide transparency into the "Bronze Layer" ingestion process, allowing operators to diagnose failures and researchers to verify data availability.

Unlike a general analytics dashboard, this tool focuses specifically on **pipeline health**, **completeness (coverage)**, and **archival status**. It is designed to be statically generated and hosted on GitHub Pages, maintaining a low operational footprint.

## 2. Core Metrics & Key Performance Indicators (KPIs)

The dashboard will visualize data primarily from the `baliza_state` DuckDB schema.

| Metric | Source Table | Description | Visual Representation |
| :--- | :--- | :--- | :--- |
| **Global Coverage** | `baliza_state.coverage` | Percentage of days with successful extraction vs. total target days. | Progress Bar / Percentage |
| **Daily Health** | `baliza_state.coverage` | Status of individual days (Complete, Empty, Missing). | GitHub-style Heatmap |
| **Ingestion Volume** | `baliza_state.coverage` | Total rows extracted per day/month. | Bar Chart (Sparkline) |
| **Pipeline Latency** | `baliza_state.runs` | Duration of extraction runs and time-since-last-run. | Status Badge (Online/Stale) |
| **Archival Lag** | `baliza_state.uploaded_to_ia` | Days extracted but not yet uploaded to Internet Archive. | Counter / "Days at Risk" |
| **Buffer Usage** | `baliza_state.extraction_checkpoint` | Number of interrupted runs requiring resumption. | Warning Indicator |

## 3. User Stories

### 3.1. The Data Engineer (Operator)
> "As an operator, I want to see if last night's scheduled extraction failed so I can inspect the logs."
*   **Needs:** Red/Green status indicators, error message snippets from `baliza_state.runs`.
*   **Actions:** Run `baliza extract` manually for failed dates.

### 3.2. The Researcher (End User)
> "As a researcher, I want to know if procurement data for November 2023 is available and archived."
*   **Needs:** Calendar/Heatmap view showing availability. Link to Internet Archive item.
*   **Actions:** Download the dataset or browse the Archive.

### 3.3. The Auditor (Stakeholder)
> "As an auditor, I want to ensure the project is actually archiving data and not just hoarding it locally."
*   **Needs:** "Uploaded vs. Local" comparison stats.

## 4. Visual Design

The design should reflect the project's backend/CLI nature ("Bronze Layer") while remaining accessible.

*   **Theme:** **Cyberpunk / Terminal Dark Mode**.
    *   *Background:* Slate-900 (`#0f172a`)
    *   *Accents:* Emerald-500 (Success), Amber-500 (Warning), Rose-500 (Failure), Indigo-500 (Info).
    *   *Font:* Monospace for numbers and identifiers (e.g., `JetBrains Mono` or `Fira Code`), Sans-serif for UI text (`Inter`).
*   **Layout:**
    1.  **Header:** Project status (Online/Maintenance), Last Updated timestamp.
    2.  **KPI Grid:** 4-card layout (Total Rows, Coverage %, Archival Lag, Last Run Status).
    3.  **Main Viz:** Full-width Daily Coverage Heatmap (Year view).
    4.  **Activity Log:** Condensed table of recent `baliza_state.runs`.
    5.  **Footer:** Links to Repo, Issues, and Internet Archive.

### 4.1. Wireframe Concept
```text
+------------------------------------------------------------------+
|  BALIZA MONITOR  [● Online]                    Last Upd: 10m ago |
+------------------------------------------------------------------+
| [ Total Rows ]  [ Coverage ]  [ Archive Lag ]  [ Buffer Size ]   |
| [ 14,205,991 ]  [   87%    ]  [   12 Days   ]  [   1.2 GB    ]   |
+------------------------------------------------------------------+
|                                                                  |
|  DATA COVERAGE (2024)                                            |
|  Jan [■■■■■■■■■■□□□□■■■■■■■■■■■■■■■■■]  82%                      |
|  Feb [■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■] 100%                      |
|  ...                                                             |
|                                                                  |
+------------------------------------------------------------------+
|  RECENT ACTIVITY                                                 |
|  10:00 AM | Extract 2024-03-01 | SUCCESS | 1,204 rows          |
|  09:45 AM | Extract 2024-03-01 | FAILED  | TimeoutError...     |
+------------------------------------------------------------------+
```

## 5. Data Sources & Integration

### 5.1. The `status.json` Schema
The dashboard will consume a static JSON file generated daily by the CLI.

```json
{
  "generated_at": "2024-03-20T10:00:00Z",
  "environment": "production",
  "stats": {
    "total_rows": 14205991,
    "total_size_gb": 4.2,
    "days_covered": 845,
    "days_archived": 830
  },
  "coverage_heatmap": {
    "2024": [1, 1, 1, 0, 0, 2, ...] // 0:Missing, 1:Complete, 2:Partial
  },
  "recent_runs": [
    {
      "id": "run_123",
      "timestamp": "2024-03-20T09:00:00Z",
      "status": "success",
      "rows": 5000
    }
  ]
}
```

### 5.2. Generation Workflow
1.  **Trigger:** GitHub Action (Daily or Post-Run).
2.  **Action:** Runs `baliza status --json > public/status.json`.
3.  **Deployment:** Commits `status.json` to the `gh-pages` branch or builds the Astro site.

## 6. Technical Recommendations

### 6.1. Stack Selection
*   **Framework:** **Astro**.
    *   *Why:* Perfect for static sites that need partial hydration. Matches the `causaganha` migration path.
    *   *Performance:* Zero JS by default, fast load times.
*   **Styling:** **TailwindCSS**.
    *   *Why:* Rapid development, consistency with existing `index.html`.
*   **Interactivity:** **Preact** (via Astro Islands).
    *   *Why:* Lightweight React alternative for handling the interactive Heatmap tooltips and filtering.
*   **Hosting:** **GitHub Pages**.

### 6.2. Comparison with Current Solution
The current `docs/dashboard/index.html` is a good MVP but lacks:
*   Route management (for future multi-page views).
*   Component reusability.
*   Build-time optimization.

## 7. Implementation Roadmap

### Phase 1: The "Lift & Shift" MVP (Week 1)
*   **Goal:** Replicate current `index.html` functionality in Astro.
*   **Tasks:**
    1.  Initialize `baliza-site` repository (or `site/` folder).
    2.  Port `index.html` layout to Astro components.
    3.  Implement type-safe `status.json` schema.
    4.  Setup GitHub Action to build and deploy.

### Phase 2: Enhanced Metrics (Month 1)
*   **Goal:** Add detailed views not present in the MVP.
*   **Tasks:**
    1.  Add `Recent Runs` table (requires new CLI export logic).
    2.  Add "Archive vs. Local" diff view.
    3.  Implement "Night Mode" toggle (defaulting to Dark).

### Phase 3: Federation (Future)
*   **Goal:** Support multiple extraction nodes.
*   **Tasks:**
    1.  Aggregate `status.json` from multiple sources.
    2.  unified dashboard for the entire ecosystem.
