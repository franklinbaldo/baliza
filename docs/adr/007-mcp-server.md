# ADR-007: Implement an MCP Server for AI-Powered Analysis

**Status:** Accepted

**Context:**
The structured data in the Gold layer is extremely valuable, but exploring it still requires technical knowledge (SQL, Python). To further democratize access and enable more intuitive forms of analysis, we can expose the data to Large Language Models (LLMs).

**Decision:**
We will implement a server compliant with the **Model Context Protocol (MCP)**, an open standard from Anthropic. This server (`baliza mcp`) will connect to the local DuckDB database and expose "tools" (like `execute_sql_query`) that an LLM (such as Claude) can use to autonomously and securely query the data on behalf of a user.

**Consequences:**
*   **Positive:**
    *   **Natural Language Querying:** Allows users to ask complex questions about the data without writing a single line of SQL.
    *   **AI-Augmented Analysis:** Unlocks advanced use cases like automatic summary generation, anomaly detection, and visualization creation by an LLM.
    *   **Security:** The implementation ensures that only read-only (`SELECT`) queries are permitted, protecting the integrity of the database.
    *   **Innovation:** Positions BALIZA at the forefront of the interaction between open data and artificial intelligence.
*   **Negative:**
    *   **Additional Component:** Adds a new service to maintain and document.
    *   **Dependency on an External Standard:** The success of the feature depends on the adoption and stability of the MCP standard.
    *   **Resources:** Running the MCP server consumes local resources (CPU/memory) while active.