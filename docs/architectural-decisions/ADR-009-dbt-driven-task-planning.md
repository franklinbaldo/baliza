# ADR-009: dbt-Driven Task Planning Architecture

**Status**: APPROVED  
**Date**: 2025-01-19  
**Authors**: Franklin Baldo, Claude Code  
**Supersedes**: ADR-002 (Task-Driven Extraction)

## Problem Statement

BALIZA's current extraction architecture generates tasks at runtime using procedural Python, creating operational and analytical limitations:

- **Performance**: 2.3s generation latency per execution (1,200+ tasks/month)
- **Observability**: No pre-execution task analysis or coverage metrics
- **Maintainability**: Task planning coupled tightly to extraction logic
- **Governance**: No versioning, auditing, or configuration management
- **Configuration Drift**: Endpoint definitions scattered across Python and dbt

## Solution: dbt-Driven Task Planning

Transform extraction tasks from runtime-generated objects into persistent data entities with clear layer separation:

| Layer | Purpose | Tables |
|-------|---------|--------|
| **Planning** | Static task generation | `task_plan`, `task_plan_meta` |
| **Runtime** | Execution tracking | `task_claims`, `task_results` |
| **Analytics** | Monitoring & reporting | `task_status`, `task_history` |

### Core Data Schema

```sql
-- planning.task_plan: Extraction task definitions
task_id           TEXT PRIMARY KEY      -- Deterministic hash
endpoint_name     TEXT NOT NULL         -- From endpoints.yaml
data_date         DATE NOT NULL         -- Target extraction date
modalidade        INTEGER               -- Procurement modality filter
status            TEXT DEFAULT 'PENDING'
plan_fingerprint  TEXT NOT NULL         -- Configuration hash
created_at        TIMESTAMP DEFAULT NOW()

-- runtime.task_claims: Atomic execution claims  
claim_id         UUID PRIMARY KEY
task_id          TEXT REFERENCES task_plan(task_id)
claimed_at       TIMESTAMP NOT NULL
expires_at       TIMESTAMP NOT NULL     -- Simple expiration (no heartbeat)
worker_id        TEXT NOT NULL
status           TEXT DEFAULT 'CLAIMED'

-- runtime.task_results: Execution outcomes
result_id        UUID PRIMARY KEY  
task_id          TEXT REFERENCES task_plan(task_id)
request_id       UUID REFERENCES pncp_requests(id)
page_number      INTEGER
records_count    INTEGER  
completed_at     TIMESTAMP NOT NULL
```

### Implementation Benefits

- **Performance**: Eliminates 2.3s task generation latency
- **Observability**: SQL-based task analysis and coverage metrics  
- **Governance**: Git-versioned plans with change detection
- **Maintainability**: Clear separation between planning and execution
- **Scalability**: DuckDB handles 100k+ tasks efficiently

## Implementation Plan

### Week 1: Foundation  
- Create unified `endpoints.yaml` configuration
- Implement plan fingerprinting in Python  
- Build basic dbt planning models

### Week 2: Runtime Infrastructure
- Deploy task claiming with expiration
- Add periodic claim reaper (simple UPDATE)
- Stream task results to runtime tables  

### Week 3: Integration
- Migrate extractor to dbt-only workflow
- Deploy incremental status aggregation  
- Create operational dashboards

### Week 4: Production Validation
- Monitor key metrics for 7 days
- Optimize performance and fix issues
- Document operational procedures

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| **Configuration drift** | Unified `endpoints.yaml` + fingerprint validation |
| **Zombie claims** | Simple expiration with periodic cleanup |
| **Concurrency issues** | Atomic claiming with `SELECT...FOR UPDATE` |
| **dbt dependency** | Already required for analytics - no new risk |

## Decision: APPROVED

**Full dbt migration without fallback** based on steelman analysis:

- Cold-start overhead empirically <1s (not 30s as feared)
- Zombie claims resolved with simple expiration + cleanup  
- Hybrid approach creates permanent configuration drift
- dbt dependency already exists for analytics stack
- Simplified architecture reduces operational complexity

---

*Architecture decision by Franklin Baldo. Bold simplicity over defensive complexity.*