# Request-Level Deduplication Strategy

## Problem Statement

After migration to DLT, we have effective **data-level deduplication** but not **request-level deduplication**:

- ✅ **Data Deduplication**: Hash-based deduplication prevents saving duplicate records to database
- ❌ **Request Deduplication**: Same HTTP requests are made repeatedly, wasting bandwidth and API quota

## Research Findings

**DLT does NOT provide request-level caching or deduplication:**
- DLT focuses on data processing, not HTTP optimization  
- REST API source makes HTTP requests regardless of existing data
- Incremental loading reduces data volume but doesn't prevent duplicate requests
- No built-in HTTP caching or URL-level state tracking

## Current Behavior (Without Request Deduplication)

```bash
# First run: Extract January 2025
baliza extract --date 2025-01
# Makes HTTP requests: pages 1-100 for January

# Second run: Same command  
baliza extract --date 2025-01
# Makes HTTP requests: pages 1-100 for January AGAIN
# But data deduplication prevents saving duplicates to database
```

**Result**: Unnecessary HTTP requests but no duplicate data stored.

## Solution Options

### Option 1: Accept Current Behavior (Recommended for MVP)

**Rationale:**
- Data deduplication prevents storage bloat (primary concern)
- PNCP API has no explicit rate limiting
- HTTP requests are relatively fast (< 1s per page)
- Implementation complexity vs. benefit trade-off

**When to use:** MVP, development, low-volume usage

### Option 2: Infrastructure-Level Caching

Implement HTTP caching outside of the application:

**A. Reverse Proxy Cache (Nginx/HAProxy)**
```nginx
location /api/consulta/ {
    proxy_pass https://pncp.gov.br;
    proxy_cache_valid 200 24h;  # Cache successful responses for 24h
    proxy_cache_key "$request_uri";
}
```

**B. Redis-based HTTP Cache**
```python
# Custom HTTP client wrapper with Redis caching
class CachedPNCPClient:
    def get(self, url):
        cached = redis.get(f"pncp:url:{hash(url)}")
        if cached:
            return json.loads(cached)
        
        response = httpx.get(url)
        redis.setex(f"pncp:url:{hash(url)}", 3600, response.text)
        return response.json()
```

**When to use:** Production, high-volume usage, multiple applications

### Option 3: Application-Level Request Tracking

Track processed URLs in database to skip redundant requests:

```python
# Create a processed_urls table
CREATE TABLE processed_urls (
    url_hash VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP,
    page_count INTEGER
);

# Before making request
def should_skip_request(url):
    url_hash = sha256(normalize_url(url))
    return db.exists("processed_urls", url_hash=url_hash)
```

**When to use:** Medium-scale production, custom control needed

### Option 4: Legacy System Integration

Keep the existing Baliza request deduplication system:

- Preserve `raw.audit_log` table for request tracking
- Maintain `EndpointExtractor._check_existing_request()` logic
- Adapt existing URL-based deduplication for DLT context

**When to use:** Preserve existing investment, gradual migration

## Recommended Implementation Plan

### Phase 1: MVP (Current State) ✅
- Use DLT with hash-based data deduplication
- Accept redundant HTTP requests
- Document the limitation
- **Status**: Completed

### Phase 2: Infrastructure Caching (Future)
- Implement Nginx reverse proxy with caching
- Cache successful API responses for 1-24 hours
- No application code changes needed
- **Effort**: 1-2 days infrastructure work

### Phase 3: Smart Request Tracking (Advanced)
- Track URL processing state in DLT pipeline state
- Skip requests for URLs processed in last N hours
- Implement cache invalidation based on data freshness requirements
- **Effort**: 1 week development

## Performance Impact Analysis

### Current State (Data Deduplication Only)
```
Full backfill extraction:
- HTTP Requests: ~50,000 (all historical pages)
- Time: ~4 hours (50k requests × 0.3s avg)
- Bandwidth: ~5GB (50k × 100KB avg response)
- Database Storage: ~2GB (after deduplication)
```

### With Request Deduplication
```
Re-run same extraction:
- HTTP Requests: ~0 (all skipped)
- Time: ~5 minutes (database queries only)  
- Bandwidth: ~0MB
- Database Storage: ~2GB (unchanged)
```

**Performance Gain**: 48x faster for re-runs, 100% bandwidth savings

## Decision Matrix

| Solution | Complexity | Performance Gain | Maintenance | Best For |
|----------|------------|------------------|-------------|----------|
| Accept Current | Low | 0% | Low | MVP, Development |
| Nginx Cache | Medium | 90%+ | Low | Production |
| Redis Cache | Medium | 95%+ | Medium | Multi-app |
| App-level Tracking | High | 98%+ | High | Custom Control |
| Legacy Integration | Very High | 99%+ | High | Migration |

## Conclusion

**For Baliza migration**: Start with **Option 1 (Accept Current Behavior)** for MVP.

**For production scale**: Implement **Option 2 (Infrastructure Caching)** with Nginx.

The current DLT implementation with hash-based data deduplication provides the essential functionality (no duplicate data storage) while keeping the implementation simple and maintainable.

Request-level deduplication can be added later as a performance optimization when the system reaches sufficient scale to justify the additional complexity.