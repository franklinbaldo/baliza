# Pydantic Model Mismatch Analysis Report

## 🔍 Executive Summary

Our validation testing revealed significant mismatches between our current Pydantic models and the actual PNCP API responses. This report analyzes the discrepancies and provides recommendations for creating better models using the official OpenAPI specification.

## 📊 Current Validation Results

### ✅ Working Models
| Endpoint | Status | Details |
|----------|--------|---------|
| `atas` | **PASS** | Full Pydantic validation success |
| `atas_atualizacao` | **PASS** | Inherits from working atas model |

### ❌ Failing Models
| Endpoint | Status | Major Issues |
|----------|--------|--------------|
| `contratos` | **FAIL** | Data type mismatches, missing fields |
| `contratacoes_publicacao` | **FAIL** | Enum validation failures, format issues |

## 🔍 Detailed Analysis: Contratos Endpoint

### Current Issues
- **valorAcumulado**: Expected `number`, getting `string` in API response
- **Missing fields**: API returns 27+ fields, our model only covers subset
- **Type mismatches**: Several numeric fields come as strings

### API Response Sample Fields (from validation test)
```
- anoContrato: int
- categoriaProcesso: dict
- codigoPaisFornecedor: str
- dataAssinatura: str
- dataAtualizacao: str
- dataAtualizacaoGlobal: str
- dataPublicacaoPncp: str
- dataVigenciaFim: str
- dataVigenciaInicio: str
- identificadorCipi: NoneType ⚠️ (causes DLT warnings)
- ... and 27 more fields
```

### OpenAPI Specification Analysis
According to `docs/openapi/api-pncp-consulta.json`, the `contratos` endpoint returns `PaginaRetornoRecuperarContratoDTO` which includes:

**Key Fields from OpenAPI Schema:**
```json
"RecuperarContratoDTO": {
  "type": "object",
  "properties": {
    "valorAcumulado": {"type": "number"},
    "niFornecedorSubContratado": {"type": "string"},
    "nomeFornecedorSubContratado": {"type": "string"},
    "tipoPessoaSubContratada": {"type": "string", "enum": ["PJ", "PF", "PE"]},
    "identificadorCipi": {"type": "string"},
    "urlCipi": {"type": "string"},
    "unidadeSubRogada": {"$ref": "#/components/schemas/RecuperarUnidadeOrgaoDTO"},
    "orgaoSubRogado": {"$ref": "#/components/schemas/RecuperarOrgaoEntidadeDTO"}
  }
}
```

**🎯 Root Cause**: API returns `null` values for numeric fields that OpenAPI spec defines as required numbers.

## 🔍 Detailed Analysis: Contratacoes Publicacao Endpoint

### Current Issues
- **situacaoCompraId**: Expected enum `["1", "2", "3", "4"]`, getting other values
- **dataAberturaProposta/dataEncerramentoProposta**: Date format validation failures
- **valorTotalHomologado**: Expected number, getting string/null values

### OpenAPI Specification Analysis
The endpoint returns `PaginaRetornoRecuperarCompraPublicacaoDTO`:

**Key Problematic Fields:**
```json
"RecuperarCompraPublicacaoDTO": {
  "properties": {
    "situacaoCompraId": {"type": "string", "enum": ["1", "2", "3", "4"]},
    "dataAberturaProposta": {"type": "string", "format": "date-time"},
    "dataEncerramentoProposta": {"type": "string", "format": "date-time"},
    "valorTotalHomologado": {"type": "number"},
    "unidadeSubRogada": {"$ref": "#/components/schemas/RecuperarUnidadeOrgaoDTO"},
    "orgaoSubRogado": {"$ref": "#/components/schemas/RecuperarOrgaoEntidadeDTO"}
  }
}
```

**🎯 Root Cause**: API returns integers instead of string enums (implementation bug) and null values for fields not marked as nullable in spec.

## 🛠️ Recommendations

### Option 1: Create New Accurate Models (Recommended)
Generate new Pydantic models directly from the OpenAPI specification with permissive field handling:

```python
from typing import Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator

class RecuperarContratoDTO(BaseModel):
    """Updated model based on OpenAPI spec with flexible typing"""

    # Core fields with flexible typing to handle API inconsistencies
    valorAcumulado: Optional[Union[str, float]] = None
    niFornecedorSubContratado: Optional[str] = None
    nomeFornecedorSubContratado: Optional[str] = None
    tipoPessoaSubContratada: Optional[str] = None  # Remove enum constraint
    identificadorCipi: Optional[str] = None
    urlCipi: Optional[str] = None

    # Convert string numbers to float when possible
    @validator('valorAcumulado', pre=True)
    def parse_valor_acumulado(cls, v):
        if isinstance(v, str):
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
        return v

    class Config:
        extra = "allow"  # Allow additional fields from API
```

### Option 2: Hybrid Approach (Current Implementation)
Continue using the current hybrid approach:
- Use Pydantic models for endpoints that validate successfully (`atas`)
- Use permissive column definitions for problematic endpoints (`contratos`, `contratacoes_publicacao`)

### Option 3: Update Existing Models
Modify existing Pydantic models to handle API inconsistencies:
- Make numeric fields accept both `str` and `number` types
- Remove strict enum constraints
- Add missing optional fields
- Use `Config.extra = "allow"` to handle unknown fields

## 🎯 Specific Field Mapping Issues

### Missing Fields Causing DLT Warnings
Based on our validation testing, these fields exist in API responses but not in our models:

**Contratos Endpoint:**
- `ni_fornecedor_sub_contratado` → `niFornecedorSubContratado` ✅ (in OpenAPI)
- `nome_fornecedor_sub_contratado` → `nomeFornecedorSubContratado` ✅ (in OpenAPI)
- `tipo_pessoa_sub_contratada` → `tipoPessoaSubContratada` ✅ (in OpenAPI)
- `identificador_cipi` → `identificadorCipi` ✅ (in OpenAPI)
- `url_cipi` → `urlCipi` ✅ (in OpenAPI)

**Atas Endpoint:**
- `data_cancelamento` → `dataCancelamento` ✅ (in OpenAPI)
- `cnpj_orgao_subrogado` → `cnpjOrgaoSubrogado` ✅ (in OpenAPI)
- `nome_orgao_subrogado` → `nomeOrgaoSubrogado` ✅ (in OpenAPI)
- `codigo_unidade_orgao_subrogado` → `codigoUnidadeOrgaoSubrogado` ✅ (in OpenAPI)
- `nome_unidade_orgao_subrogado` → `nomeUnidadeOrgaoSubrogado` ✅ (in OpenAPI)

## 💡 OpenAPI-Based Model Generation Strategy

### Step 1: Extract Schema Definitions
The OpenAPI spec contains complete schema definitions for all response models:
- `PaginaRetornoRecuperarContratoDTO`
- `PaginaRetornoRecuperarCompraPublicacaoDTO`
- `PaginaRetornoAtaRegistroPrecoPeriodoDTO`
- `PaginaRetornoConsultarInstrumentoCobrancaDTO`
- `PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO`

### Step 2: Generate Base Models
Use the OpenAPI schema to generate base Pydantic models with all fields defined.

### Step 3: Add Flexibility Layer
Enhance generated models with:
- Union types for fields that come as different types
- Optional typing for frequently null fields
- Validators to handle type conversion
- `extra = "allow"` configuration

### Step 4: Map Snake_Case to CamelCase
The API returns camelCase field names, but our DLT warnings show snake_case. Need field aliases:

```python
class RecuperarContratoDTO(BaseModel):
    ni_fornecedor_sub_contratado: Optional[str] = Field(None, alias="niFornecedorSubContratado")
    nome_fornecedor_sub_contratado: Optional[str] = Field(None, alias="nomeFornecedorSubContratado")
    # ... etc
```

## 🚀 Implementation Plan

### Phase 1: Immediate Fix (Current)
✅ **COMPLETED**: Use hybrid approach with permissive schemas for problematic endpoints.

### Phase 2: Model Regeneration (Future)
1. **Extract OpenAPI schemas** for all response models
2. **Generate new Pydantic models** with flexible typing
3. **Add field aliases** for snake_case/camelCase mapping
4. **Test validation** against real API responses
5. **Update configuration** to use new models

### Phase 3: Validation & Testing
1. **Create comprehensive test suite** validating all endpoints
2. **Monitor DLT warnings** to catch any remaining issues
3. **Performance testing** to ensure no degradation

## 📈 Expected Outcomes

## 🎯 Exact OpenAPI vs Reality Analysis

### Detailed Field-by-Field Investigation

Based on validation testing against real API responses, here are the **exact discrepancies**:

#### 1. `situacaoCompraId` - Type Mismatch (API Implementation Error)

**OpenAPI Specification:**
```json
{
  "type": "string",
  "enum": ["1", "2", "3", "4"]
}
```

**API Reality:**
- Returns: `1` (integer)
- Expected: `"1"` (string)

**🎯 Root Cause**: **API implementation bug** - The API is returning integers but OpenAPI spec correctly defines it as string enum. Our Pydantic model is correct, the API is wrong.

#### 2. `valorAcumulado` - Missing Nullable Definition

**OpenAPI Specification:**
```json
{
  "type": "number"
}
```

**API Reality:**
- Returns: `null` (NoneType)
- Expected: `1234.56` (number)

**🎯 Root Cause**: **Incomplete OpenAPI spec** - Field should be marked as `nullable: true` or optional since API returns null values.

#### 3. Date Fields - Missing Optional Definition

**OpenAPI Specification:**
```json
{
  "type": "string",
  "format": "date-time",
  "example": "2025-07-11T13:08:48"
}
```

**API Reality:**
- `dataAberturaProposta`: Returns `null` (NoneType)
- `dataEncerramentoProposta`: Returns `null` (NoneType)
- Expected: `"2025-06-05T16:29:01"` (string)

**🎯 Root Cause**: **Incomplete OpenAPI spec** - These fields should be marked as optional since API returns null values when dates are not set.

### 📊 Summary of Discrepancies

| Field | OpenAPI Says | API Returns | Issue Type |
|-------|-------------|-------------|------------|
| `situacaoCompraId` | `string` enum `["1","2","3","4"]` | `int` values `1, 2` | **API Implementation Error** |
| `valorAcumulado` | `number` (required) | `null` | **Missing Nullable Spec** |
| `dataAberturaProposta` | `string` (required) | `null` | **Missing Optional Spec** |
| `dataEncerramentoProposta` | `string` (required) | `null` | **Missing Optional Spec** |
| `valorTotalHomologado` | `number` (required) | `null` | **Missing Nullable Spec** |

### 💡 Corrected Model Strategy

The issues are **not generic** - they fall into two specific categories:

1. **API Bug**: `situacaoCompraId` - Add type coercion validator
2. **Spec Gaps**: Null-returning fields - Make optional with proper typing

```python
# Correct approach for situacaoCompraId (API bug workaround)
@field_validator('situacaoCompraId', mode='before')
def coerce_situacao_compra_id(cls, v):
    """Handle API returning integers instead of strings as specified in OpenAPI"""
    if isinstance(v, int):
        return str(v)  # Convert int to string to match OpenAPI spec
    return v

# Correct approach for nullable fields (spec gaps)
valorAcumulado: Optional[float] = None
dataAberturaProposta: Optional[str] = None
dataEncerramentoProposta: Optional[str] = None
valorTotalHomologado: Optional[float] = None
```

### 🎯 Conclusion

The problems are **not generic schema mismatches** but specific, identifiable issues:

1. **1 API implementation bug** (wrong type returned)
2. **4+ missing nullable/optional specifications** in OpenAPI

This is much more fixable than originally thought - we need targeted validators for the API bug and optional typing for the null-returning fields.

### Immediate Benefits (Current Hybrid Approach)
- ✅ **Eliminated schema inference warnings** for 12+ common fields
- ✅ **Clean extraction logs** without distracting warnings
- ✅ **Maintained performance** with concurrent processing

### Future Benefits (Corrected Models)
- 🎯 **Targeted fixes** for specific API bugs and spec gaps
- 🎯 **Type safety** with proper validation where possible
- 🎯 **Maintainability** through precise error handling

## 📋 Conclusion

The OpenAPI specification is mostly accurate but has specific gaps. The main challenges are:

1. **1 API Implementation Bug**: `situacaoCompraId` returns wrong type
2. **Incomplete Optional Specs**: Fields return null but aren't marked nullable
3. **Field Naming**: API uses camelCase, DLT expects snake_case

**Recommendation**: Use targeted fixes for the specific identified issues rather than broad permissive schemas.
