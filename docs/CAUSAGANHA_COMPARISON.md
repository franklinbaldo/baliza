# Baliza vs CausaGanha Comparison

> Analysis comparing [baliza](https://github.com/franklinbaldo/baliza) with [causaganha](https://github.com/franklinbaldo/causaganha) to extract patterns and insights.

## Executive Summary

Both projects share a common technical DNA but serve different domains:
- **Baliza**: Brazilian public procurement data (PNCP)
- **CausaGanha**: Brazilian judicial decisions and lawyer ratings

CausaGanha has evolved several architectural patterns that could accelerate Baliza's development.

---

## Shared Technical Foundation

| Aspect | Both Projects |
|--------|---------------|
| **Language** | Python 3.11+ |
| **CLI Framework** | Typer |
| **Package Manager** | uv |
| **Database** | DuckDB |
| **Data Format** | Parquet |
| **Testing** | pytest-bdd (BDD) |
| **Type Checking** | mypy (strict) |
| **Linting** | ruff |
| **HTTP Client** | httpx |
| **Data Distribution** | Internet Archive |
| **CI/CD** | GitHub Actions |
| **Mission** | Brazilian data transparency |

---

## Key Architectural Differences

### 1. Sync vs Async Architecture

| Baliza | CausaGanha |
|--------|------------|
| Synchronous httpx | Async-first (asyncio everywhere) |
| Sequential pagination | Concurrent request batching |
| ~100 req/min | ~1000+ req/min |

**CausaGanha Pattern:**
```python
async def fetch_all_courts(courts: list[str]) -> list[CourtData]:
    tasks = [fetch_court_data(court) for court in courts]
    return await asyncio.gather(*tasks)
```

### 2. Code Organization

| Baliza | CausaGanha |
|--------|------------|
| 3 source files | Multi-layer modular architecture |
| `cli_simple.py`, `extractor.py`, `utils.py` | `api/`, `storage/`, `analysis/`, `pipeline/`, `scoring/` |
| Simple, pragmatic | Scalable, maintainable |

### 3. Data Processing

| Baliza | CausaGanha |
|--------|------------|
| Raw DuckDB SQL | Ibis Framework |
| Manual query optimization | Lazy evaluation, auto-optimization |
| Adequate for current scale | 10-100x faster for complex analytics |

### 4. Testing Scale

| Baliza | CausaGanha |
|--------|------------|
| 5 focused BDD scenarios | 459+ BDD scenarios |
| Single tier | Prioritized P1-P5 hierarchy |
| "Ship early, iterate" | Comprehensive coverage |

### 5. AI/LLM Integration

| Baliza | CausaGanha |
|--------|------------|
| None | Pydantic AI (provider-agnostic) |
| N/A | Structured output extraction |
| N/A | RAG + LLM hybrid analysis |

### 6. Automation

| Baliza | CausaGanha |
|--------|------------|
| Daily/monthly cron jobs | Jules system (23 AI personas) |
| Manual development | Autonomous development (every 15 min) |
| Human-driven PR review | Auto-merge on CI success |

---

## Actionable Insights for Baliza

### High Priority

#### 1. Async-First Extraction (5-10x speedup)

Migrate `extractor.py` to async httpx for concurrent API requests:

```python
# Before (sync)
for page in range(total_pages):
    response = self.client.get(url, params={"pagina": page})
    process(response.json())

# After (async)
async def fetch_all_pages(self, total_pages: int) -> list[dict]:
    async with httpx.AsyncClient() as client:
        tasks = [
            client.get(url, params={"pagina": page})
            for page in range(total_pages)
        ]
        responses = await asyncio.gather(*tasks)
        return [r.json() for r in responses]
```

**Estimated effort**: 4 hours
**Impact**: 5-10x faster extraction

#### 2. BDD Test Prioritization

Expand from 5 tests to comprehensive coverage using priority tiers:

```
tests/features/
├── P1_core/              # Must always pass
│   ├── extraction.feature
│   └── export.feature
├── P2_operations/        # Should pass
│   ├── verification.feature
│   └── resilience.feature
├── P3_advanced/          # Nice to have
│   ├── incremental.feature
│   └── performance.feature
└── P4_quality/           # Monitoring
    └── data_validation.feature
```

**Estimated effort**: 8 hours
**Impact**: Comprehensive test coverage

### Medium Priority

#### 3. Structured Logging

Add structlog for JSON-compatible, aggregatable logs:

```python
import structlog

logger = structlog.get_logger()

logger.info("extraction_complete",
           contracts=1500,
           duration_ms=3400,
           start_date="2024-01-01",
           end_date="2024-01-31")
```

**Benefits**:
- Better debugging with context
- Log aggregation compatibility
- Structured data for analytics

**Estimated effort**: 1 hour
**Impact**: Better observability

#### 4. Ibis Framework for Analytics

Add Ibis for complex analytical queries in the `verify` command:

```python
import ibis

con = ibis.duckdb.connect("baliza.duckdb")
contratos = con.table("contratos")

# Lazy evaluation - only fetches needed data
coverage_gaps = (
    contratos
    .filter(contratos.dataPublicacao.between(start, end))
    .group_by(contratos.dataPublicacao.truncate('day'))
    .aggregate(count=contratos.count())
    .filter(lambda t: t.count < expected_threshold)
)
```

**Estimated effort**: 3 hours
**Impact**: Faster analytics, cleaner code

#### 5. Multi-Parquet Architecture

Separate exports for better query performance:

```
data/
├── contratos/              # Raw contract data
│   └── ano=YYYY/mes=MM/
├── orgaos/                 # Organization reference data
│   └── orgaos.parquet
└── agregados/              # Pre-computed statistics
    └── resumo_mensal.parquet
```

**Benefits**:
- 3-5x faster queries (smaller files)
- Reprocessing flexibility
- Better caching

**Estimated effort**: 2 hours
**Impact**: Query performance

### Future Considerations

#### 6. AI-Powered Anomaly Detection

Use Pydantic AI for procurement anomaly analysis:

```python
from pydantic import BaseModel
from pydantic_ai import Agent

class ContractAnalysis(BaseModel):
    anomaly_score: float
    risk_factors: list[str]
    price_deviation_pct: float
    recommendation: str

agent = Agent(model="gemini-2.5-flash")

async def analyze_contract(contract: dict) -> ContractAnalysis:
    return await agent.run(
        f"Analyze this procurement contract for anomalies: {contract}",
        result_type=ContractAnalysis
    )
```

**Use cases**:
- Price outlier detection
- Suspicious vendor patterns
- Contract splitting detection
- Unusual timing patterns

#### 7. Jules-Style Automation

Consider autonomous AI personas for development:

| Persona | Role |
|---------|------|
| 🔧 Refactor | Auto-fix linting, type errors |
| 🛡️ Sentinel | Security vulnerability scanning |
| 🧪 BDD Specialist | Test coverage expansion |

---

## Implementation Roadmap

### Phase 0: GitHub Actions Patterns (COMPLETED)

- [x] **Concurrency control** - Prevent overlapping workflow runs
- [x] **D-1 priority extraction** - Always extract yesterday first
- [x] **State persistence via artifacts** - Resume from previous database state
- [x] **High-frequency extraction workflow** - Every 30 minutes continuous extraction
- [x] **Timeout protection** - Prevent runaway workflows

### Phase 1: Quick Wins (Week 1)

- [ ] Add structlog for logging
- [ ] Add pytest markers for test tiers (`@pytest.mark.tier0`, etc.)
- [ ] Create separate Parquet for organizations

### Phase 2: Performance (Week 2-3)

- [ ] Migrate extractor to async httpx
- [ ] Add Ibis for analytical queries
- [ ] Implement concurrent page fetching

### Phase 3: Testing (Week 4)

- [ ] Expand BDD scenarios with P1-P5 hierarchy
- [ ] Add integration tests for edge cases
- [ ] Add performance benchmarks

### Phase 4: Advanced (Future)

- [ ] AI-powered anomaly detection (optional)
- [ ] Multi-parquet export architecture
- [ ] Consider Jules-style automation

---

## Conclusion

CausaGanha provides a battle-tested blueprint for:
1. **Async architecture** - Essential for API-heavy workloads
2. **Structured testing** - Priority-based BDD hierarchy
3. **Modern tooling** - Ibis, structlog, Pydantic AI
4. **Automation** - AI-assisted development

The biggest immediate wins for Baliza are:
1. **Async extraction** (5-10x speedup)
2. **Structured logging** (better debugging)
3. **Test tier expansion** (comprehensive coverage)

These patterns can be adopted incrementally without disrupting Baliza's "ship early, iterate" philosophy.
