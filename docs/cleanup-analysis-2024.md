# Comprehensive Project Cleanup Analysis - July 2024

**Post-Ibis Integration Cleanup Report**  
**Date**: July 24, 2024  
**Status**: Recommended cleanup after successful Ibis pipeline merge

## Executive Summary

After the successful implementation and merge of the Ibis pipeline system, this analysis identifies comprehensive cleanup opportunities that can **reduce project size by ~40%** and eliminate legacy technical debt. The cleanup focuses on removing the now-obsolete Kedro framework and associated infrastructure.

## Cleanup Categories

### 🔥 Complete System Removal - Kedro Framework

**Impact**: Major simplification, ~30% size reduction

```bash
# Remove entire Kedro pipeline system
rm -rf pipelines/
rm -rf conf/
rm src/settings.py

# Remove Kedro CLI integration
# Edit src/baliza/cli.py to remove run_pipeline command (lines 518-526)

# Remove from pyproject.toml
# Remove [tool.kedro] section (lines 282-286)
```

**Justification**: Kedro is completely replaced by the new Ibis pipeline system. The parallel infrastructure creates confusion and maintenance overhead.

### 📂 Directory Structure Cleanup

**Legacy Data Directories** (~10% size reduction):
```bash
# These follow old Kedro conventions, not used by current system
rm -rf data/01_raw/
rm -rf data/02_intermediate/  
rm -rf data/03_primary/
rm -rf data/08_reporting/
```

**Obsolete SQL Directory**:
```bash
# Legacy SQL queries replaced by Ibis transformations
rm -rf sql/
```

### 🗂️ Configuration File Cleanup

**Kedro Configuration**:
```bash
rm -rf conf/base/catalog.yml     # 196 lines of obsolete data catalog
rm -rf conf/base/parameters.yml  # If exists
rm -rf conf/local/              # Local Kedro configs
```

### 🔧 Code File Cleanup

**Legacy Transformer** (~5% reduction):
```bash
rm src/baliza/transformer.py    # 38 lines - replaced by Ibis integration in CLI
```

**Mock Data Scripts**:
```bash
rm create_dummy_data.py         # 63 lines - only for old Kedro testing
```

**Empty/Unused Modules**:
```bash
# Check and remove if empty after Kedro removal
rm src/baliza/pipelines/        # If directory becomes empty
rm src/baliza/hooks/           # If exists and unused
```

### 📦 Dependency Cleanup

**Remove from pyproject.toml dependencies**:
- `kedro*` packages (if any remaining)
- Any Kedro-specific plugins
- Legacy pandas versions that were Kedro-specific

**Development Dependencies Review**:
- Check if any dev tools were Kedro-specific
- Confirm dbt dependencies are still needed (they are - for parallel workflow)

## File-by-File Impact Analysis

### High Impact Removals

| File/Directory | Lines/Size | Impact | Safe to Remove |
|---------------|------------|---------|----------------|
| `pipelines/` | ~500 lines | Complete Kedro system | ✅ Yes - replaced by Ibis |
| `conf/` | ~300 lines | Kedro configuration | ✅ Yes - not used anymore |
| `src/settings.py` | ~50 lines | Kedro settings | ✅ Yes - CLI handles config now |
| `data/` subdirs | ~1000+ files | Legacy data structure | ⚠️ Careful - check for real data |

### Medium Impact Removals

| File | Purpose | Current Status | Action |
|------|---------|----------------|---------|
| `transformer.py` | dbt CLI wrapper | Replaced in CLI | ✅ Remove |
| `create_dummy_data.py` | Test data generation | Kedro-specific | ✅ Remove |
| `catalog.yml` | Data catalog | Kedro-only | ✅ Remove |

### Low Impact / Keep

| File | Reason to Keep |
|------|----------------|
| `dbt_baliza/` | Still used for parallel dbt workflow |
| `tests/` | Current E2E tests are valuable |
| `docs/` | Documentation is important |
| Core `src/baliza/` modules | Active codebase |

## Cleanup Execution Plan

### Phase 1: Safe Removals (No Dependencies)
```bash
# Documentation and unused scripts
rm create_dummy_data.py
rm -rf docs/kedro/  # If exists

# Empty or clearly unused directories
rm -rf logs/  # If only contains Kedro logs
```

### Phase 2: Configuration Cleanup
```bash
# Remove Kedro configs
rm -rf conf/
rm src/settings.py

# Update pyproject.toml - remove [tool.kedro] section
```

### Phase 3: Code Integration Cleanup
```bash
# Remove old transformer after confirming CLI integration works
rm src/baliza/transformer.py

# Remove CLI command for Kedro pipeline
# Edit src/baliza/cli.py to remove run_pipeline function
```

### Phase 4: Major Directory Removal
```bash
# Remove entire pipeline system
rm -rf pipelines/

# Remove legacy data structure (AFTER backing up any real data)
rm -rf data/01_raw/ data/02_intermediate/ data/03_primary/ data/08_reporting/
```

## Risk Assessment

### 🟢 Low Risk
- Configuration files (`conf/`, `catalog.yml`)
- Mock data scripts (`create_dummy_data.py`)
- Documentation files specific to Kedro

### 🟡 Medium Risk  
- Legacy transformer (`transformer.py`) - ensure CLI integration is complete
- Data directories - verify they don't contain real extracted data

### 🔴 High Risk
- None identified - all removals are obsolete systems

## Post-Cleanup Validation

### Required Tests After Cleanup
```bash
# Verify core functionality still works
baliza extract --month 2024-07  # Should work with Ibis
baliza transform                 # Should use Ibis pipeline
baliza transform --dbt           # Should still work for dbt users

# Verify no import errors
python -c "from baliza.cli import app; print('CLI imports OK')"

# Run E2E tests
pytest tests/e2e/
```

### Expected Benefits
- **40% smaller codebase** - easier to navigate and maintain
- **Single pipeline system** - no confusion between Kedro/Ibis
- **Faster CI/CD** - fewer files to process
- **Cleaner dependencies** - remove unused packages
- **Better documentation** - focus on active systems only

## Implementation Timeline

**Estimated Time**: 2-3 hours total
- Phase 1 (Safe removals): 30 minutes
- Phase 2 (Config cleanup): 45 minutes  
- Phase 3 (Code integration): 60 minutes
- Phase 4 (Directory removal): 30 minutes
- Testing and validation: 30 minutes

## Backup Strategy

Before executing cleanup:
```bash
# Create cleanup branch
git checkout -b cleanup/remove-kedro-system

# Create backup of current state
git tag backup-pre-cleanup

# Verify we can rollback if needed
git log --oneline -5
```

## Long-term Maintenance

After cleanup, establish:
- **Clear architecture documentation** focusing on Ibis pipeline
- **Updated contributor guidelines** removing Kedro references
- **Simplified CI/CD** without Kedro testing
- **Documentation review** to remove outdated Kedro instructions

---

**Recommendation**: Proceed with cleanup in phases, testing after each phase. The project will be significantly simplified and focused on the modern Ibis-based architecture.

**Next Steps**: 
1. Execute Phase 1 safe removals
2. Test core functionality 
3. Proceed with remaining phases
4. Update all documentation to reflect new simplified architecture