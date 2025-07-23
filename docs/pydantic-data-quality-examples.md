# Pydantic Data Quality Examples

## Visão Geral

Os modelos Pydantic são essenciais para garantir que apenas dados válidos e corretos sejam inseridos no banco. Esta é uma das principais vantagens da nova arquitetura direct-to-table.

## Validações Implementadas

### 1. Validação de Tipos Automática

```python
class BronzeContrato(BaseModel):
    # Pydantic automaticamente valida e converte tipos
    ano_contrato: int                    # String "2024" → int 2024
    valor_inicial: Optional[Decimal]     # String "123.45" → Decimal(123.45)
    data_assinatura: Optional[date]      # String "2024-07-23" → date(2024, 7, 23)
    receita: Optional[bool]              # String "true" → bool True
    
    # Se conversão falhar, ValidationError é lançado
    # Exemplo: ano_contrato="ABC" → ValidationError: Input should be a valid integer
```

### 2. Validação de Formato e Tamanho

```python
class BronzeContrato(BaseModel):
    orgao_cnpj: Optional[str] = Field(None, max_length=14)  # Máximo 14 caracteres
    unidade_uf_sigla: Optional[str] = Field(None, max_length=2)  # Máximo 2 caracteres
    tipo_pessoa: Optional[str] = Field(None, max_length=2)  # PJ, PF, PE apenas
    
    model_config = ConfigDict(
        str_strip_whitespace=True,  # Remove espaços automaticamente
        validate_assignment=True    # Valida também em atribuições posteriores
    )
```

### 3. Validações Customizadas (Avançadas)

```python
from pydantic import field_validator, model_validator
import re

class BronzeContrato(BaseModel):
    orgao_cnpj: Optional[str] = None
    ni_fornecedor: Optional[str] = None
    valor_inicial: Optional[Decimal] = None
    data_vigencia_inicio: Optional[date] = None
    data_vigencia_fim: Optional[date] = None
    
    @field_validator('orgao_cnpj', 'ni_fornecedor')
    @classmethod
    def validate_cnpj_cpf(cls, v: Optional[str]) -> Optional[str]:
        """Valida formato e dígitos verificadores de CNPJ/CPF."""
        if v is None:
            return v
            
        # Remove formatação
        digits_only = re.sub(r'[^\d]', '', v)
        
        # Valida CNPJ (14 dígitos)
        if len(digits_only) == 14:
            if not cls._validate_cnpj_digits(digits_only):
                raise ValueError(f"CNPJ inválido: {v}")
            return digits_only
            
        # Valida CPF (11 dígitos)  
        elif len(digits_only) == 11:
            if not cls._validate_cpf_digits(digits_only):
                raise ValueError(f"CPF inválido: {v}")
            return digits_only
            
        else:
            raise ValueError(f"CNPJ/CPF deve ter 11 ou 14 dígitos: {v}")
    
    @field_validator('valor_inicial', 'valor_parcela', 'valor_global')
    @classmethod
    def validate_positive_values(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Garante que valores monetários não são negativos."""
        if v is not None and v < 0:
            raise ValueError(f"Valor não pode ser negativo: {v}")
        return v
    
    @model_validator(mode='after')
    def validate_date_consistency(self) -> 'BronzeContrato':
        """Valida consistência entre datas."""
        if (self.data_vigencia_inicio and self.data_vigencia_fim and 
            self.data_vigencia_inicio > self.data_vigencia_fim):
            raise ValueError("Data de início não pode ser posterior à data de fim")
        return self
    
    @classmethod
    def _validate_cnpj_digits(cls, cnpj: str) -> bool:
        """Valida dígitos verificadores do CNPJ."""
        # Implementação dos cálculos dos dígitos verificadores
        # ... código de validação ...
        return True  # Simplificado para exemplo
    
    @classmethod
    def _validate_cpf_digits(cls, cpf: str) -> bool:
        """Valida dígitos verificadores do CPF.""" 
        # Implementação dos cálculos dos dígitos verificadores
        # ... código de validação ...
        return True  # Simplificado para exemplo
```

### 4. Validação Contra ENUMs Oficiais

```python
class BronzeContratacao(BaseModel):
    modalidade_id: Optional[int] = None
    modalidade_nome: Optional[str] = None
    situacao_compra_id: Optional[str] = None
    
    @field_validator('modalidade_id')
    @classmethod
    def validate_modalidade(cls, v: Optional[int]) -> Optional[int]:
        """Valida se modalidade existe nos ENUMs oficiais."""
        if v is None:
            return v
            
        # IDs válidos de modalidade conforme PNCP
        valid_modalidades = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
        
        if v not in valid_modalidades:
            raise ValueError(f"Modalidade inválida: {v}. Deve ser uma de: {valid_modalidades}")
        return v
    
    @field_validator('situacao_compra_id')
    @classmethod  
    def validate_situacao(cls, v: Optional[str]) -> Optional[str]:
        """Valida se situação existe nos ENUMs oficiais."""
        if v is None:
            return v
            
        # Situações válidas conforme PNCP
        valid_situacoes = {"1", "2", "3", "4"}
        
        if v not in valid_situacoes:
            raise ValueError(f"Situação inválida: {v}. Deve ser uma de: {valid_situacoes}")
        return v
```

### 5. Transformações e Sanitização

```python
class BronzeContrato(BaseModel):
    orgao_razao_social: Optional[str] = None
    objeto_contrato: Optional[str] = None
    
    @field_validator('orgao_razao_social', 'objeto_contrato', mode='before')
    @classmethod
    def sanitize_strings(cls, v: Optional[str]) -> Optional[str]:
        """Sanitiza e normaliza strings."""
        if v is None or v == "":
            return None
            
        # Remove espaços extras e normaliza
        sanitized = ' '.join(v.strip().split())
        
        # Converte strings vazias para None
        return sanitized if sanitized else None
    
    @field_validator('orgao_cnpj', mode='before')
    @classmethod
    def normalize_cnpj(cls, v: Optional[str]) -> Optional[str]:
        """Remove formatação de CNPJ."""
        if v is None:
            return v
        # Remove pontos, barras e hífens    
        return re.sub(r'[.\-/]', '', v.strip())
```

## Exemplos de Uso no Pipeline

### 1. Parsing com Validação Automática

```python
def _parse_contratos(self, contratos_data: List[dict]) -> Tuple[int, List[str]]:
    """Parse contratos com validação Pydantic."""
    records_parsed = 0
    errors = []
    
    for contrato_data in contratos_data:
        try:
            # Pydantic faz toda a validação automaticamente
            bronze_contrato = BronzeContrato.from_api_response(contrato_data)
            
            # Se chegou aqui, dados estão 100% válidos
            self._insert_bronze_contrato(bronze_contrato)
            records_parsed += 1
            
        except ValidationError as e:
            # Erro detalhado de quais campos falharam na validação
            error_msg = f"Validation error for contrato {contrato_data.get('numeroControlePNCP')}: {e}"
            errors.append(error_msg)
            
            # Log para debug com contexto completo
            self._log_parse_error(
                "contratos_parsing", 
                "", 
                contrato_data, 
                error_msg, 
                "validation_error"
            )
    
    return records_parsed, errors
```

### 2. Mensagens de Erro Detalhadas

```python
# Exemplo de erro Pydantic para debugging:
"""
ValidationError: 2 validation errors for BronzeContrato
orgao_cnpj
  String should have at most 14 characters [type=string_too_long, input_value='123456789012345', input_type=str]
valor_inicial  
  Input should be a valid number, unable to parse string as a number [type=decimal_parsing, input_value='valor_inválido', input_type=str]
"""
```

### 3. IDE Support e Type Safety

```python
# IDE automaticamente sabe os tipos e oferece autocomplete
contrato = BronzeContrato.from_api_response(response_data)

# Type hints funcionam perfeitamente
contrato.valor_inicial  # IDE sabe que é Optional[Decimal]
contrato.data_assinatura  # IDE sabe que é Optional[date]
contrato.orgao_cnpj  # IDE sabe que é Optional[str]

# Erros de tipo são detectados antes da execução
contrato.valor_inicial = "string"  # Mypy/IDE mostra erro de tipo
```

## Vantagens dos Modelos Pydantic

### 1. **Data Quality Garantida**
- 100% dos dados inseridos passaram por validação
- Tipos corretos garantidos no banco de dados
- Formato consistente (CNPJs sempre 14 dígitos, etc.)

### 2. **Debugging Simplificado**
- Mensagens de erro claras e detalhadas
- Contexto completo sobre qual campo falhou
- Stack trace preservado para análise

### 3. **Documentação Viva**
- Schemas servem como documentação da estrutura
- IDE mostra tipos e constraints automaticamente
- Exemplos de uso ficam sempre atualizados

### 4. **Performance**
- Validação acontece em memória antes da inserção
- Falhas são detectadas cedo, evitando rollbacks
- Parsing eficiente com conversões automáticas

### 5. **Manutenibilidade**
- Mudanças no schema são refletidas nos tipos
- Refactoring fica mais seguro com type hints
- Testes ficam mais confiáveis

## Integração com Fase 3B

Esta validação Pydantic é uma das principais inovações da Fase 3B:

1. **API Response** → **Pydantic Model** → **Validação** → **Database Insert**
2. **Apenas dados válidos** chegam ao banco
3. **Erros de parsing** ficam isolados na tabela `pncp_parse_errors`
4. **Quality metrics** podem ser calculadas baseadas nas validações

O resultado é um sistema muito mais robusto e confiável, onde problemas de qualidade de dados são detectados e tratados automaticamente no momento da ingestão.