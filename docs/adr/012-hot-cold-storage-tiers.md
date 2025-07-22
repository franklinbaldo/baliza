# ADR-012: Hot-Cold Storage Tiers

## Status
Accepted

## Context
BALIZA handles large volumes of historical public procurement data. Different data has different access patterns:
- **Hot data** - Recent data (last 90 days) accessed frequently for real-time queries
- **Cold data** - Historical data (>90 days) accessed infrequently for analysis
- **Archive data** - Very old data (>2 years) rarely accessed, stored for compliance

Storage optimization requires different strategies for each tier to balance performance and cost.

## Decision
We will implement a three-tier storage architecture optimized for different access patterns.

**Tier Definitions:**
1. **Hot Tier** (last 90 days)
   - DuckDB native format for fastest queries
   - Global ZSTD compression but avoid --strict during writes
   - Real-time upserts and frequent access
   - Optimized for query performance over storage efficiency

2. **Cold Tier** (90 days - 2 years)  
   - Compressed DuckDB format with aggressive optimization
   - Row-group tuning for better compression
   - Read-only with periodic batch updates
   - Balanced between storage and query performance

3. **Archive Tier** (>2 years)
   - Parquet files with maximum compression
   - S3/cloud storage via httpfs extension
   - External views without local copy (`read_parquet('s3://...')`)
   - Optimized purely for storage cost

**Automated Tier Management:**
- Daily job moves data between tiers based on age
- `CHECKPOINT` after tier moves to reclaim space
- Transparent views to query across tiers

## Consequences

### Positive
- Optimal performance for recent data (hot tier)
- Significant storage savings for historical data
- Cost-effective archiving for compliance data
- Transparent querying across all tiers
- Automatic data lifecycle management

### Negative
- Complex data lifecycle management
- Potential query performance variation across tiers
- Dependency on cloud storage for archive tier
- Need for robust backup/recovery across tiers

## Implementation Notes
- Hot tier: Standard DuckDB with `SET default_compression='zstd'`
- Cold tier: Aggressive compression + row-group optimization  
- Archive tier: Parquet with `ROW_GROUP_SIZE 500k/128MB` for S3
- Use `ON CONFLICT DO NOTHING` to handle duplicate detection
- `CHECKPOINT` after `DELETE` operations to reclaim space

## References
- Database Optimization Plan Section 4: "Implementação de Arquivamento em Tiers"
- ADR-010: Compression Heuristics (different strategies per tier)
- ADR-001: DuckDB adoption (enables efficient tiering)