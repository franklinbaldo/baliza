# ADR-010: DuckDB Compression Heuristics First

## Status
Accepted

## Context
Database storage optimization is critical for BALIZA's performance with large public procurement datasets. There are two approaches to compression:
1. **Manual compression specification** - Force specific algorithms per column
2. **Automatic heuristics** - Let DuckDB choose optimal compression per column

Initial plans included extensive manual compression specifications (`USING COMPRESSION dictionary`, `USING COMPRESSION fsst`, etc.), but research shows DuckDB's automatic heuristics are highly effective and often outperform manual choices.

## Decision
We will rely primarily on DuckDB's automatic compression heuristics rather than forcing specific compression algorithms.

**Compression Strategy:**
1. **Trust DuckDB heuristics** - Let DuckDB choose compression automatically per column
2. **Global ZSTD setting** - Use `SET default_compression='zstd'` for global override when needed
3. **ENUMs are sufficient** - Don't add `USING COMPRESSION dictionary` to ENUM types (they're already dictionaries internally)
4. **Avoid --strict in hot writes** - Apply ZSTD in nightly CHECKPOINT, not during active writes
5. **Manual override only when proven** - Force specific compression only when heuristics demonstrably fail

**What we WON'T do:**
- ❌ Force FSST on text columns (let DuckDB decide)
- ❌ Force bitpacking on numeric columns (heuristics handle this better)  
- ❌ Apply `USING COMPRESSION dictionary` to ENUMs (redundant)
- ❌ Set compression per table when global setting works

## Consequences

### Positive
- Leverages DuckDB's sophisticated automatic optimization
- Reduces maintenance overhead of manual compression tuning
- Better performance in most cases due to data-aware heuristics
- Simpler DDL without compression specifications
- Future-proof as DuckDB heuristics improve

### Negative
- Less explicit control over storage format
- Harder to predict exact storage characteristics
- May need manual override in edge cases

## Implementation Notes
- Configure `SET default_compression='zstd'` globally
- Use `CHECKPOINT` after bulk operations to apply compression
- Monitor `pragma_storage_info()` to verify compression effectiveness
- Only override heuristics when benchmarks show clear benefit

## References
- Database Optimization Plan: "Princípios de Compressão: Heurística Automática do DuckDB"
- [DuckDB Lightweight Compression Documentation](https://duckdb.org/2022/10/28/lightweight-compression.html)