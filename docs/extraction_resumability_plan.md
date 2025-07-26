Of course. Here is a much-improved `docs/extraction_resumability_plan.md` that provides a concrete, actionable plan for making the Baliza extraction pipeline efficient and resumable.

This new document replaces the higher-level `docs/request-deduplication-strategy.md` with a detailed, DLT-native architectural design.

---

# Extraction Resumability and Efficiency Plan

## 1. Executive Summary

This document outlines a stateful, DLT-native architecture to make the Baliza extraction pipeline **efficient, resumable, and intelligent**. The current pipeline successfully prevents duplicate *data* storage but suffers from inefficient *request* duplication, re-fetching data that already exists.

The proposed solution is to implement a **State Manager** that tracks successfully completed extractions. This will enable a rewritten **Gap Detector** to identify the precise date ranges and modalities that are missing, ensuring the DLT pipeline only requests new data.

**Key Objectives:**

1.  **Eliminate Redundant API Calls:** Only fetch data that is not already present.
2.  **Enable True Resumability:** Failed or interrupted pipelines can be restarted and will seamlessly continue from where they left off.
3.  **Increase Extraction Speed:** Subsequent runs will be significantly faster as they only process deltas.
4.  **Leverage DLT-native Features:** Properly utilize `dlt`'s state and incremental loading capabilities.

This plan transitions Baliza from a stateless, idempotent extractor to a stateful, intelligent one, drastically improving performance and robustness.

## 2. Problem Statement

The current DLT-based pipeline has two major inefficiencies:

1.  **Request-Level Inefficiency:** The pipeline is idempotent at the **data level** (thanks to hash-based deduplication) but not at the **request level**. Running `baliza extract --days 7` twice in a row will make the exact same API calls both times. The second run's data is simply discarded, wasting bandwidth, API quota, and time.

2.  **Non-Functional Gap Detection:** The existing `src/baliza/extraction/gap_detector.py` is a placeholder. Its `FIXME` comments confirm it does not perform any actual gap analysis and always returns the full requested date range. This makes the entire extraction process stateless and naive.

3.  **Inefficient State Tracking:** The current method of creating `.completed` files for each `(endpoint, month)` is brittle, hard to query, and does not support granular tracking (e.g., by day or by modalidade).

As a result, the pipeline is not truly resumable and cannot perform efficient incremental updates.

## 3. Proposed Architecture: A Stateful DLT Pipeline

We will introduce a state management layer that integrates with the DLT pipeline to track progress and guide subsequent extractions.

### 3.1. Core Components

```mermaid
flowchart TD
    subgraph CLI
        A[baliza extract --days 7]
    end
    
    subgraph Baliza Pipeline
        B(1. Parse Date/Endpoint Options)
        C(2. Gap Detector)
        D(3. DLT Source Generation)
        E(4. DLT Pipeline Execution)
        F(5. State Manager)
    end

    subgraph Artifacts
        G[State Store<br>(pipeline_state.json)]
        H[Parquet Files]
    end

    A --> B
    B --> C
    C -- reads from --> G
    C -- identifies gaps --> D
    D -- generates sources for gaps --> E
    E -- writes to --> H
    E -- on success, updates --> F
    F -- writes to --> G
```

### 3.2. The State Manager & State Store

We will create a `StateManager` class responsible for abstracting the read/write operations to a central state file (`pipeline_state.json`) located in the DLT pipeline's working directory.

**`pipeline_state.json` Structure:**

```json
{
  "version": "1.0",
  "last_updated": "2025-07-26T10:00:00Z",
  "completed_ranges": {
    "contratos": [
      ["20240101", "20240331"],
      ["20240501", "20240515"]
    ],
    "contratacoes_publicacao": {
      "6": [["20240101", "20240229"]],
      "8": [["20240101", "20240131"]]
    },
    "atas": [
      ["20230101", "20240630"]
    ]
  },
  "incremental_watermarks": {
    "contratos_atualizacao": "2025-07-25T14:30:00Z",
    "atas_atualizacao": "2025-07-25T14:28:00Z"
  }
}
```

**Key Features:**

-   **Completed Ranges:** Stores a list of `[start_date, end_date]` strings for each endpoint. For endpoints requiring `modalidade`, the state is nested under the modalidade code.
-   **Watermarks:** Stores the last successfully processed value for `dlt.sources.incremental` cursors (e.g., `dataAtualizacaoGlobal`).
-   **Atomicity:** Writes to the state file will be atomic to prevent corruption.

### 3.3. The Rewritten Gap Detector

The `gap_detector.py` module will be rewritten to be the intelligent core of the pipeline.

**Logic:**

1.  **Input:** A requested extraction range (`start_date`, `end_date`), a list of endpoints, and optional modalities.
2.  **Action:**
    -   Load the current state from `pipeline_state.json` using the `StateManager`.
    -   For each endpoint/modalidade combination, calculate the "missing" date ranges by subtracting the `completed_ranges` from the `requested_range`.
    -   This is a set-difference operation on date ranges.
3.  **Output:** A list of `DataGap` objects representing the precise, non-overlapping periods that need to be fetched.

## 4. Implementation Plan

### Phase 1: Implement the State Manager

-   **Task:** Create `src/baliza/extraction/state_manager.py`.
-   **Class `StateManager`:**
    -   `__init__(self, pipeline: dlt.Pipeline)`: Locates the state file in the pipeline's working directory.
    -   `load_state() -> dict`: Reads and validates the state JSON.
    -   `save_state(self, state: dict)`: Atomically writes the state JSON.
    -   `get_completed_ranges(self, endpoint: str, modalidade: int = None) -> list`: Returns sorted, merged date ranges.
    -   `mark_range_completed(self, endpoint: str, start_date: str, end_date: str, modalidade: int = None)`: Adds a new range and merges overlaps.
-   **Goal:** A robust, tested class for state manipulation. The inefficient `.completed` file logic in `pipeline.py` will be removed.

### Phase 2: Rewrite the Gap Detector

-   **Task:** Overhaul `src/baliza/extraction/gap_detector.py`.
-   **Function `find_extraction_gaps`:**
    -   Will now instantiate `StateManager`.
    -   Implement the range-difference algorithm.
        -   Example: Request `[Jan 1, Jan 31]`, State has `[Jan 10, Jan 20]`.
        -   Output gaps: `[Jan 1, Jan 9]` and `[Jan 21, Jan 31]`.
    -   It will correctly handle per-modalidade state for endpoints like `contratacoes_publicacao`.
-   **Goal:** `find_extraction_gaps` returns an accurate list of `DataGap` objects, or an empty list if the requested range is already complete.

### Phase 3: Integrate into the DLT Pipeline

-   **Task:** Modify `src/baliza/extraction/pipeline.py`.
-   **Function `pncp_source`:**
    -   The call to `find_extraction_gaps` will now be functional.
    -   It will iterate over the returned `DataGap` objects and dynamically generate a `dlt` source for each one.
-   **Task:** Modify `src/baliza/cli.py`.
-   **Function `extract`:**
    -   After a successful `pipeline.run()`, it will call the `StateManager` to update the state with the newly completed ranges.
-   **Goal:** The end-to-end flow is complete. The pipeline is now stateful.

### Phase 4: DLT-native Incremental Sync

-   **Task:** Implement incremental sync for `*_atualizacao` endpoints.
-   **Action:**
    -   Create a new DLT resource function decorated with `@dlt.resource`.
    -   Inside this function, use `dlt.sources.incremental` with `dataAtualizacaoGlobal` as the `cursor_path`.
    -   The `StateManager` will be extended to manage `last_value` watermarks for these incremental endpoints.
-   **Goal:** Utilize DLT's most efficient sync method for endpoints that support it, reducing the reliance on date-range gap detection for those specific cases.

## 5. Deprecation and Cleanup

-   **To be Removed:**
    -   The placeholder logic in `gap_detector.py`.
    -   The `.completed` file creation/checking logic in `pipeline.py` (`mark_extraction_completed`, `is_extraction_completed`, etc.).
-   **To be Deprecated:**
    -   The `docs/request-deduplication-strategy.md` file will be replaced by this document.

## 6. Benefits

-   **Efficiency:** A backfill of one year of data, if interrupted halfway, will resume from the exact day it stopped, not from the beginning of the year.
-   **Speed:** Daily runs to fetch the "last 7 days" will only make API calls for the single most recent day not yet processed.
-   **Robustness:** The state is managed centrally and atomically, making it resilient to failures.
-   **Cost Savings:** Reduces egress bandwidth from the PNCP API and compute time on the client side.
-   **Correctness:** Finally implements the "smart" extraction promised in the project's vision.