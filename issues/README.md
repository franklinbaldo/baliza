# BALIZA Issues Status Summary

Last updated: 2025-07-20 (Updated with new dbt model issues)

## 📊 Overview

This directory contains architectural and design issues identified in the BALIZA project. Issues are organized by current status and priority.

## ✅ Resolved Issues (Moved to `closed/`)

### Security & Architecture (High Priority - FIXED)
- `mcp_server.md` - **CRITICAL SQL injection vulnerability fixed**
- `cli.md` - **Monolithic run command refactored** into modular components  
- `loader.md` - **Comprehensive error handling** and retry mechanisms added
- `config.md` - **Hardcoded configuration addressed** with endpoints.yaml
- `endpoints_yaml.md` - **Implemented as part of dbt-driven architecture**

## 🚨 High Priority Issues (Active)

### 1. `extractor.py` - NEEDS SIGNIFICANT REFACTORING
**Priority:** HIGH  
**Status:** Complex methods violate SRP, tight coupling makes testing difficult
- Extract methods are 300+ lines, handle multiple concerns
- Strong coupling with PNCPWriter/PNCPTaskPlanner  
- Mixed UI and business logic

### 2. `generate_task_plan_sql.md` - ARCHITECTURAL FLAW
**Priority:** CRITICAL  
**Status:** Hardcoded configuration duplicates endpoints.yaml
- Creates maintenance nightmare, violates DRY principle
- Requires dbt-external-tables implementation
- Blocks configuration consistency

## 🔧 Medium Priority Issues (Maintenance)

### dbt Models & Configuration
- `task_plan_sql.md` - Hardcoded defaults, redundant indexes
- `task_plan_meta_sql.md` - Schema and validation issues  
- `planning_schema_yml.md` - Table structure concerns
- `dbt_project_yml.md` - Configuration redundancy

### 🆕 NEW: dbt Data Models (Schema & Quality)
- `gold_schema_yml.md` - Incomplete schema definitions, missing documentation
- `silver_schema_yml.md` - Missing schemas for several models, inconsistent descriptions
- `silver_itens_contratacao_sql.md` - Hardcoded enum mappings, brittle unique keys
- `mart_compras_beneficios_sql.md` - Hardcoded benefit type IDs, incremental logic issues
- `silver_atas_sql.md` - Silver layer data transformation issues
- `silver_contratacoes_sql.md` - Procurement data processing concerns
- `silver_contratos_sql.md` - Contract data modeling issues
- `silver_dim_organizacoes_sql.md` - Organization dimension table issues
- `silver_dim_unidades_orgao_sql.md` - Organizational unit dimension issues
- `silver_documentos_sql.md` - Document processing concerns
- `silver_fact_contratacoes_sql.md` - Procurement fact table issues
- `silver_fact_contratos_sql.md` - Contract fact table concerns

### Core Components  
- `task_claimer.md` - **PARTIALLY RESOLVED** - Some hardcoding remains
- `plan_fingerprint.md` - Concerns separation needed
- `pncp_writer.md` - Monolithic database initialization
- `pncp_client.md` - Configuration and error handling
- `transformer.md` - Hardcoded dbt commands

### Data Models
- `bronze_content_analysis_sql.md` - Schema and performance
- `bronze_pncp_raw_sql.md` - Raw data handling  
- `bronze_pncp_source_yml.md` - Source configuration
- `data_quality_monitoring_sql.md` - Quality metrics

## 💾 Low Priority Issues (Enhancements)

### Utilities & Support
- `utils.md` - Limited scope, implicit dependencies
- `content_utils.md` - Hardcoded values, performance optimizations
- `enums.md` - Validation and extensibility
- `pncp_task_planner.md` - Legacy component concerns
- `profiles_yml.md` - dbt profile configuration
- `pyproject_toml.md` - Dependency and config redundancy

### Extract & Transform
- `extract_organization_data_sql.md` - Data extraction logic

## 🎯 Next Steps & Recommendations

### Immediate Actions (High Priority)
1. **Refactor extractor.py** - Break down complex methods, implement dependency injection
2. **Fix generate_task_plan.sql** - Implement dbt-external-tables for endpoints.yaml integration
3. **Complete task_claimer.py** - Address remaining hardcoded values and error handling

### 🆕 NEW: Data Quality Issues (Medium Priority)
4. **Centralize enum definitions** - Create dbt seed tables from Python enums to eliminate hardcoded values
5. **Complete schema documentation** - Add missing schema definitions for all silver/gold models
6. **Improve incremental strategies** - Fix brittle assumptions about data ordering
7. **Add comprehensive column documentation** - Enhance downstream usability

### Medium Term (Architecture)
1. **Implement state machine** for task status management
2. **Add dependency injection** throughout CLI components  
3. **Create data access layer** to eliminate hardcoded SQL
4. **Separate UI logic** from business components
5. **🆕 Implement dbt-seed integration** - Dynamically generate enum mappings

### Long Term (Quality)
1. **Schema validation** for all YAML configurations
2. **Performance optimization** for large-scale processing
3. **Enhanced error recovery** mechanisms
4. **Comprehensive testing** for refactored components
5. **🆕 Data quality monitoring** - Automated tests for enum consistency and data integrity

## 📈 Impact Assessment

**Security Issues:** ✅ **RESOLVED** - Critical vulnerabilities fixed  
**Architecture Issues:** 🚨 **HIGH** - Coupling and complexity need attention  
**🆕 Data Quality Issues:** 🔧 **MEDIUM** - 12 new dbt model issues identified  
**Maintainability:** 🔧 **MEDIUM** - Hardcoded values and mixed concerns  
**Performance:** 💾 **LOW** - Optimizations can be deferred

### 🆕 **New Issues Summary:**
- **12 dbt model files** with data quality and schema issues
- **Common problems:** Hardcoded enum values, incomplete documentation, brittle incremental logic
- **Impact:** Affects data integrity and downstream analytics reliability
- **Priority:** Medium (doesn't block core functionality but affects data quality)

---

**Note:** Issues marked as resolved have been moved to `issues/closed/` directory. Active issues are prioritized by impact on system reliability, maintainability, and development velocity.