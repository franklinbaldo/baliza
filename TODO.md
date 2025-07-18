# BALIZA TODO - E2E First Strategy

**Sapere aude** — Alinhado com nosso ADR: **E2E tests only**.

---

## 🎯 ADR CONSTRAINT: E2E TESTS ONLY

**Architectural Decision**: Baliza usa **apenas testes E2E** para validação. Sem unit tests, sem mocks para validação final.

**Implicação**: Todas as ferramentas devem **apoiar** os testes E2E, não substituí-los.

---

## 🧪 E2E TESTING IMPROVEMENTS

### 1. E2E Test Stability - ⚠️ CRITICAL
**Problem**: Current E2E tests hit real PNCP API but são instáveis.

**Real Impact**:
- Tests fail por network transiente
- Rate limiting pode bloquear development
- CI/CD fica instável por falhas não-determinísticas
- Desenvolvedores evitam rodar testes

**Solution**: Tornar E2E tests mais **resilientes**:
```python
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

@retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter())
async def test_e2e_extract_real_data():
    """E2E test usando API real com retry para stability"""
    # Test real extraction for 1 day only
    result = await extractor.extract_data(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1)
    )
    assert result["total_records_extracted"] > 0
```

**Action Items**:
- [ ] Add `tenacity` retry logic to E2E tests
- [ ] Create focused E2E test for short date ranges
- [ ] Add timeout protection to E2E tests
- [ ] Validate real data extraction end-to-end

**Priority**: 🔥 **HIGH** - Critical for CI/CD stability

---

## 🛠️ DEVELOPER EXPERIENCE (Supporting E2E)

### 2. Development Tools - ⚠️ TACTICAL
**Purpose**: Apoiar desenvolvimento **sem substituir** E2E validation.

**Problem**: E2E tests são lentos para feedback durante desenvolvimento.

**Strategy**: Use development tools that **support** E2E testing:

#### A. Fast Development Feedback (Optional)
```python
# Para desenvolvimento local apenas - NÃO para validação
import respx

@respx.mock
def test_error_handling_dev():
    """Quick dev test - apoiar desenvolvimento, não validação"""
    respx.get("https://pncp.gov.br/api/consulta/v1/contratos").mock(
        return_value=httpx.Response(500, text="Server Error")
    )
    # Test error handling logic quickly
```

#### B. Runtime Data Validation (Critical for E2E)
```python
# Pydantic para validar dados em tempo real durante E2E
from pydantic import BaseModel

class PNCPResponse(BaseModel):
    totalRegistros: int
    data: list

def test_e2e_with_validation():
    """E2E test com validação de schema em runtime"""
    response = await client.fetch_contracts()
    
    # Validate schema during E2E - catch API changes immediately
    validated = PNCPResponse.model_validate(response)
    assert validated.totalRegistros >= 0
```

**Action Items**:
- [ ] Add `pydantic` models for runtime validation in E2E tests
- [ ] (Optional) Add `respx` for quick development feedback
- [ ] Keep E2E tests as the **only** validation authority

**Priority**: 🟡 **MEDIUM** - Supports development, doesn't replace E2E

---

## 📚 DOCUMENTATION

### 3. README Update - ⚠️ NECESSARY
**Problem**: README doesn't reflect new architecture.

**Missing**:
- New module structure (config.py, utils.py, etc.)
- Updated installation instructions
- Usage examples with new CLI
- Architecture diagram

**Action Items**:
- [ ] Update README with new architecture
- [ ] Add usage examples for `baliza extract`
- [ ] Document new module structure
- [ ] Add architecture diagram

**Priority**: 🟡 **MEDIUM** - Important for new users

---

## 🎯 E2E-FIRST ASSESSMENT

### What's Actually Needed (E2E Context):
1. **E2E Test Stability** 🔥 - Make E2E tests resilient com `tenacity`
2. **Runtime Data Validation** 🟡 - `pydantic` para catch API changes durante E2E
3. **Documentation** 🟡 - Important for adoption

### What Can Be Skipped:
1. **Unit Tests** 🟢 - ADR explicitly forbids
2. **Mocks for Validation** 🟢 - ADR explicitly forbids
3. **S3 Upload Pipeline** 🟢 - No real need yet
4. **Monitoring/Observability** 🟢 - Premature optimization

### Optional (Development Support):
1. **Development Mocks** 🟢 - Optional, para speed up development cycle
2. **Error Scenario Testing** 🟢 - Optional, para test edge cases locally

---

## 📝 REALISTIC NEXT STEPS (E2E-Aligned)

### Sprint 1 (1-2 days)
1. **Stabilize E2E Tests** - Add `tenacity` retry logic
2. **Add Runtime Validation** - `pydantic` models for schema validation
3. **Update README** - Document E2E testing strategy

### Done ✅
- Core functionality works and is E2E tested
- Architecture is clean and modular
- `tenacity` already implemented in production code
- Performance is adequate for real usage

**Philosophy**: *"E2E tests are the source of truth"* — All tools must support, not replace, E2E validation.

---

## 🏆 CONCLUSION

The codebase is in excellent shape and aligns with **E2E-first ADR**. The only **real** remaining work is:

1. **E2E Test Stability** - Make tests resilient to network issues
2. **Runtime Data Validation** - Catch API changes during E2E execution
3. **Documentation** - README update

**Key Insight**: Tools like `respx` and `pydantic` **support** E2E testing, they don't replace it. `tenacity` makes E2E tests more stable. `pydantic` catches API changes during E2E execution.

**Ship it!** 🚀 — The system works E2E, the architecture is solid, and the remaining work is tactical.