# ADR-009: SQL Extraction Strategy

## Status
Accepted

## Context
BALIZA currently has SQL queries embedded directly in Python code, making database operations difficult to maintain, version, and optimize. This creates several problems:
- SQL queries are scattered across multiple Python files
- No version control or migration strategy for database changes
- Difficult to optimize queries without code changes
- No clear separation between data logic and application logic
- Hard to test SQL queries independently

## Decision
We will extract all inline SQL from Python code into organized `.sql` files with a standardized structure and loading mechanism.

**Key Principles:**
1. **Complete separation** of SQL from Python application code
2. **Parameterized templates** using safe string substitution
3. **Organized directory structure** by SQL operation type
4. **Standardized file headers** with metadata and performance notes
5. **Centralized SQL loader** for consistent query execution

**Directory Structure:**
```
sql/
├── ddl/           # Data Definition Language
├── dml/           # Data Manipulation (inserts, updates, deletes)  
├── analytics/     # Analytical queries and reports
├── maintenance/   # Cleanup, optimization, archiving
└── migrations/    # Version-controlled schema changes
```

## Consequences

### Positive
- Clear separation between data logic and application logic
- SQL queries can be version controlled and migrated independently
- Easier to optimize and performance test individual queries
- Database changes can be reviewed by SQL experts
- Enables proper database migration workflows
- SQL queries can be tested independently of Python code

### Negative
- Additional indirection when reading code
- Need to maintain SQLLoader utility
- Risk of parameter injection if not implemented carefully
- Migration effort to extract existing SQL

## References
- Database Optimization Plan Section 2.1-2.3
- ADR-003: Medallion Architecture (related data organization)