Of course. Here is a strategic roadmap that builds upon the technical plan, outlining the vision for Baliza in clear, phased stages. This document is designed to align contributors, manage expectations, and showcase the project's future potential.

---

# `docs/roadmap.md`

# 🛣️ Baliza Project Roadmap

## 1. Vision & Guiding Principles

The vision for Baliza is to become the **definitive, reliable, and accessible historical archive of Brazilian public procurement data**. We aim to empower journalists, researchers, auditors, and citizens by removing the technical barriers to accessing and analyzing this critical information.

Our development is guided by these principles:

-   🛡️ **Reliability over Features:** The core extraction pipeline must be rock-solid and trustworthy before we add new functionality.
-   🎯 **Simplicity & Accessibility:** The tool should be easy to use, and the data it produces must be in open, standard formats (Parquet) that are immediately useful.
-   ✅ **Completeness & Trust:** We will strive to capture 100% of the available data and provide tools to verify its quality and integrity.
-   🤝 **Community-Driven:** Baliza should be a foundation that others can build upon. We will prioritize features that enable a wider ecosystem of tools and analysis.

## 2. Current Status (As of Q3 2024)

BALIZA v2.0 has successfully established a modern technical foundation:

-   **Technology:** Migrated to a DLT (Data Load Tool) pipeline.
-   **Coverage:** Supports all 12 PNCP API endpoints.
-   **Output:** Produces clean, typed Parquet files.
-   **Core Limitation:** The pipeline is **stateless and inefficient**. It re-fetches data on every run, relying on data-level deduplication to discard duplicates. This is slow and wasteful.

This roadmap outlines the path to transform Baliza from a functional tool into an efficient, intelligent, and indispensable resource.

---

## 🗺️ The Roadmap

### Phase 1: Foundation - Resumability and Efficiency (Now: Q3-Q4 2024)

**Goal:** Fix the core inefficiency. Make the pipeline stateful, smart, and truly resumable. This is our highest priority.

**Key Deliverables:** (Based on the [Extraction Resumability Plan](./extraction_resumability_plan.md))

-   `[ ]` ⚙️ **Implement the State Manager:** Create a robust class to manage the pipeline's state (e.g., in a `pipeline_state.json` file), tracking which date ranges have been successfully extracted for each endpoint and modality.
-   `[ ]` 🧠 **Rewrite the Gap Detector:** Overhaul the existing placeholder to intelligently compare the user's request against the stored state, identifying the *exact* missing date ranges to fetch.
-   `[ ]` 🧩 **Integrate into the DLT Pipeline:** Modify the pipeline to use the `GapDetector`'s output, dynamically generating DLT sources only for the identified gaps.
-   `[ ]` 🔄 **Update State on Success:** Ensure the `StateManager` is called after a successful pipeline run to update the `completed_ranges`, making the process truly resumable.
-   `[ ]` 📊 **Refine the `baliza status` CLI:** The status command will be updated to read from the new state file, providing a clear and accurate picture of data completeness.

**Outcome of Phase 1:** Baliza will be **orders of magnitude faster** on subsequent runs. Failed or interrupted extractions will continue precisely where they left off. The core promise of an efficient backup tool will be realized.

---

### Phase 2: Enhancement - Usability and Data Quality (Next: Q1 2025)

**Goal:** Make the tool more user-friendly and build trust in the data by adding quality checks and better feedback.

**Key Deliverables:**

-   `[ ]` 💯 **Data Quality & Expectations:**
    -   Integrate `dlt`'s "expectations" to validate data against rules (e.g., `CNPJ` format, non-null values).
    -   Generate a data quality report after each run, highlighting any anomalies or validation failures.
-   `[ ]` 🚀 **Improved CLI Experience:**
    -   Provide more detailed progress bars, showing page numbers and record counts.
    -   Display a summary table after extraction: `X records fetched, Y duplicates skipped, Z new records saved`.
-   `[ ]` 📚 **User-Friendly Documentation:**
    -   Create a "Getting Started" guide with tutorials for common analysis tasks (e.g., finding the largest contracts, tracking a supplier).
    -   Publish documentation to a public site using MkDocs or similar.
-   `[ ]` 📈 **Pre-packaged Analytics Starter Kit:**
    -   Include a Jupyter Notebook in the repository with example queries using DuckDB and Polars to help users start their analysis immediately.

**Outcome of Phase 2:** Users will not only get the data easily but will also have a higher degree of confidence in its quality and a clearer path to generating insights.

---

### Phase 3: Ecosystem - Scale and Community (Later: 2025 and Beyond)

**Goal:** Evolve Baliza from a standalone tool into a foundational piece of the open data ecosystem in Brazil.

**Key Deliverables:**

-   `[ ]` 🌐 **Public Data Hub:**
    -   Publish the extracted Parquet datasets to a public, queryable platform (e.g., Hugging Face Datasets, AWS Open Data, Google BigQuery Public Datasets). This is the ultimate goal for accessibility.
-   `[ ]` 🐳 **Containerization:**
    -   Provide an official Docker image for Baliza, allowing it to be deployed easily in automated, server-based environments.
-   `[ ]` 🤝 **Formalize Contribution & Governance:**
    -   Create a detailed `CONTRIBUTING.md` guide.
    -   Establish clear issue and pull request templates.
-   `[ ]` 🔌 **API / Web Interface (Experimental):**
    -   Develop a simple FastAPI interface to serve basic queries on the extracted data or to display the current extraction status.
-   `[ ]` 🔗 **Integrations with BI Tools:**
    -   Provide guides and configurations for connecting tools like Metabase or Superset directly to the generated data lake.

**Outcome of Phase 3:** Baliza will dramatically lower the barrier to entry for procurement analysis, enabling a broad audience to hold power accountable without needing to run any code themselves.

## How to Contribute

Your help is crucial! Whether you're a developer, a data analyst, or just a passionate citizen, you can contribute:

1.  **Start with Phase 1:** The most impactful contribution right now is to help implement the resumability features.
2.  **Pick an Issue:** Check out the [issue tracker](https://github.com/franklinbaldo/baliza/issues) for tasks labeled `good first issue` or `help wanted`.
3.  **Improve Documentation:** Found something confusing? Open a PR to make it clearer!
4.  **Use the Data:** Create an analysis, write a story, build a visualization, and share it with us