# ADR-006: Split PSA Table Architecture for Content Deduplication

**Status**: Approved  
**Date**: 2025-07-19  
**Deciders**: Franklin Baldo, Claude  

## Context

The current BALIZA extraction system stores all PNCP API responses in a monolithic `psa.pncp_raw_responses` table that combines request metadata with response content. As the database grows (currently 43MB), we've identified significant inefficiencies:

1. **Content Duplication**: Same JSON responses stored multiple times for different requests (retries, pagination overlap, etc.)
2. **Query Performance**: Metadata queries must scan large TEXT fields unnecessarily  
3. **Storage Overhead**: Estimated 20-40% storage waste due to duplicate content
4. **Analytics Limitations**: Difficult to analyze request patterns vs content patterns separately

## Decision

We will split the monolithic `psa.pncp_raw_responses` table into two optimized tables:

### New Architecture

#### Table 1: `psa.pncp_requests` (Request Metadata)
```sql
CREATE TABLE psa.pncp_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    endpoint_url VARCHAR NOT NULL,
    endpoint_name VARCHAR NOT NULL,
    request_parameters JSON,
    response_code INTEGER NOT NULL,
    response_headers JSON,
    data_date DATE,
    run_id VARCHAR,
    total_records INTEGER,
    total_pages INTEGER,
    current_page INTEGER,
    page_size INTEGER,
    content_id UUID REFERENCES psa.pncp_content(id),
    -- Indexes for common query patterns
    INDEX idx_endpoint_date_page (endpoint_name, data_date, current_page),
    INDEX idx_response_code (response_code),
    INDEX idx_content_id (content_id)
) WITH (compression = "zstd");
```

#### Table 2: `psa.pncp_content` (Deduplicated Content)
```sql
CREATE TABLE psa.pncp_content (
    id UUID PRIMARY KEY, -- UUIDv5 based on content hash
    response_content TEXT NOT NULL,
    content_sha256 VARCHAR(64) NOT NULL UNIQUE, -- For integrity verification
    content_size_bytes INTEGER,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_count INTEGER DEFAULT 1, -- How many requests reference this content
    -- Indexes for content queries
    INDEX idx_content_hash (content_sha256),
    INDEX idx_first_seen (first_seen_at)
) WITH (compression = "zstd");
```

### UUIDv5 Content Identification

Content IDs will be generated using UUIDv5 with:
- **Namespace**: Custom BALIZA namespace UUID
- **Name**: SHA-256 hash of normalized response content
- **Benefits**: Deterministic, collision-resistant, enables efficient deduplication

```python
def generate_content_id(content: str) -> str:
    """Generate UUIDv5 based on content hash."""
    import hashlib
    import uuid
    
    # BALIZA namespace UUID (generated once)
    BALIZA_NAMESPACE = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    
    # Normalize and hash content
    content_normalized = content.strip()
    content_hash = hashlib.sha256(content_normalized.encode('utf-8')).hexdigest()
    
    # Generate deterministic UUIDv5
    return str(uuid.uuid5(BALIZA_NAMESPACE, content_hash))
```

## Expected Benefits

### 🚀 Performance Improvements
- **20-40% storage reduction** through content deduplication
- **Faster metadata queries** (no large TEXT field scans)
- **Selective content loading** (fetch content only when needed)
- **Better index efficiency** on smaller request table

### 📊 Analytics Enhancement  
- **Request pattern analysis** without loading content
- **Content reuse metrics** (reference_count tracking)
- **Separate backup strategies** for metadata vs content
- **Improved monitoring** and debugging capabilities

### 🛡️ Data Integrity
- **Content verification** via SHA-256 hashes
- **Referential integrity** through foreign key constraints
- **Duplicate detection** at insert time
- **Orphaned content cleanup** capabilities

## Implementation Strategy

### Phase 1: Schema Implementation
1. **Create new tables** alongside existing table
2. **Implement content hashing utilities**
3. **Update PNCPWriter** for dual-table inserts
4. **Add deduplication logic** in writer

### Phase 2: Application Updates
1. **Update extractor queries** to use JOINs
2. **Modify CLI stats** for new schema
3. **Update dbt models** for bronze layer
4. **Add content management utilities**

### Phase 3: Migration
1. **Create migration script** for existing 43MB data
2. **Validate deduplication rates** (expect 20-40% reduction)
3. **Performance testing** before/after comparison
4. **Rollback capability** if issues arise

### Phase 4: Cleanup
1. **Drop old table** after validation
2. **Optimize indexes** based on usage patterns  
3. **Update documentation** and monitoring
4. **Create maintenance procedures**

## Migration Plan

### Backwards Compatibility
During migration, we'll maintain compatibility by:
1. **Dual-write** to both old and new schemas
2. **Feature flags** to switch between schemas
3. **Validation queries** to ensure data consistency
4. **Rollback procedures** if performance degrades

### Data Migration Script
```sql
-- Step 1: Migrate unique content
INSERT INTO psa.pncp_content (id, response_content, content_sha256, content_size_bytes, first_seen_at)
SELECT 
    generate_content_uuid(response_content) as id,
    response_content,
    sha256(response_content) as content_sha256,
    length(response_content) as content_size_bytes,
    MIN(extracted_at) as first_seen_at
FROM psa.pncp_raw_responses 
WHERE response_content IS NOT NULL
GROUP BY response_content;

-- Step 2: Migrate request metadata
INSERT INTO psa.pncp_requests (...)
SELECT 
    id, extracted_at, endpoint_url, endpoint_name,
    request_parameters, response_code, response_headers,
    data_date, run_id, total_records, total_pages, current_page, page_size,
    generate_content_uuid(response_content) as content_id
FROM psa.pncp_raw_responses;
```

## Risks and Mitigations

### Identified Risks
1. **JOIN Performance**: Queries requiring content need JOINs
   - **Mitigation**: Proper indexing and query optimization
2. **Migration Complexity**: 43MB data migration 
   - **Mitigation**: Incremental migration with validation
3. **Application Changes**: All queries need updates
   - **Mitigation**: Phased rollout with backwards compatibility

### Success Metrics
- **Storage Reduction**: Target 20-40% database size reduction
- **Query Performance**: Metadata queries 50%+ faster
- **Deduplication Rate**: Measure actual content reuse
- **Migration Success**: Zero data loss, minimal downtime

## Alternatives Considered

1. **Keep Monolithic Table**: Simplest but doesn't address core issues
2. **Content Compression Only**: Limited benefits, doesn't solve duplication
3. **External Content Store**: Complex, introduces new dependencies

## Implementation Timeline

- **Week 1**: Schema design, UUIDv5 implementation, writer updates
- **Week 2**: Migration script, testing, dbt model updates  
- **Week 3**: Performance validation, rollout, documentation

## Conclusion

This split architecture addresses fundamental inefficiencies in our current design while providing a more scalable foundation for future growth. The benefits significantly outweigh the implementation complexity, especially as the database continues to grow with daily extractions.

The UUIDv5-based content deduplication is an elegant solution that ensures deterministic IDs while enabling efficient storage and retrieval patterns that will serve BALIZA well as it scales.