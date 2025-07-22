# Relatório de Auditoria de Compliance PNCP

## 1. Tabelas de Domínio (ENUMs) Encontradas

### Instrumento Convocatório
Encontrados 3 valores.
- `1`: Edital
- `2`: Aviso de Contratação Direta
- `3`: Ato que autoriza a Contratação Direta
### Modalidade de Contratação
Encontrados 13 valores.
- `1`: Leilão - Eletrônico
- `2`: Diálogo Competitivo
- `3`: Concurso
... e mais 10 valores

### Modo de Disputa
Encontrados 6 valores.
- `1`: Aberto
- `2`: Fechado
- `3`: Aberto-Fechado
... e mais 3 valores

### Critério de Julgamento
Encontrados 108 valores.
- `1`: Menor preço
- `2`: Maior desconto
- `4`: Técnica e preço
... e mais 105 valores

## 2. Análise Detalhada do Schema OpenAPI (via Prance)

### Schema: `RespostaErroValidacaoDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 5
- **Campos Obrigatórios**: 0
### Schema: `PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 6
- **Campos Obrigatórios**: 0
### Schema: `PlanoContratacaoComItensDoUsuarioDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 9
- **Campos Obrigatórios**: 0
### Schema: `PlanoContratacaoItemDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 21
- **Campos Obrigatórios**: 0
### Schema: `ContratacaoFonteOrcamentariaDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 4
- **Campos Obrigatórios**: 0
### Schema: `RecuperarAmparoLegalDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 3
- **Campos Obrigatórios**: 0
### Schema: `RecuperarCompraDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 38
- **Campos Obrigatórios**: 0
### Schema: `RecuperarOrgaoEntidadeDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 4
- **Campos Obrigatórios**: 0
### Schema: `RecuperarUnidadeOrgaoDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 6
- **Campos Obrigatórios**: 0
### Schema: `Categoria`
- **Tipo**: `object`
- **Total de Propriedades**: 2
- **Campos Obrigatórios**: 0
### Schema: `ConsultarInstrumentoCobrancaDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 17
- **Campos Obrigatórios**: 0
### Schema: `EventoNotaFiscalConsultaDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 4
- **Campos Obrigatórios**: 0
### Schema: `ItemNotaFiscalConsultaDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 9
- **Campos Obrigatórios**: 0
### Schema: `NotaFiscalEletronicaConsultaDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 20
- **Campos Obrigatórios**: 0
### Schema: `PaginaRetornoConsultarInstrumentoCobrancaDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 6
- **Campos Obrigatórios**: 0
### Schema: `RecuperarContratoDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 37
- **Campos Obrigatórios**: 0
### Schema: `TipoContrato`
- **Tipo**: `object`
- **Total de Propriedades**: 2
- **Campos Obrigatórios**: 0
### Schema: `TipoInstrumentoCobrancaDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 6
- **Campos Obrigatórios**: 0
### Schema: `PaginaRetornoRecuperarContratoDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 6
- **Campos Obrigatórios**: 0
### Schema: `PaginaRetornoRecuperarCompraPublicacaoDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 6
- **Campos Obrigatórios**: 0
### Schema: `RecuperarCompraPublicacaoDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 34
- **Campos Obrigatórios**: 0
### Schema: `AtaRegistroPrecoPeriodoDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 23
- **Campos Obrigatórios**: 0
### Schema: `PaginaRetornoAtaRegistroPrecoPeriodoDTO`
- **Tipo**: `object`
- **Total de Propriedades**: 6
- **Campos Obrigatórios**: 0
## 3. Mapeamento Preliminar: ENUMs vs. Campos OpenAPI

- ✅ **Modalidade de Contratação** → `codigoModalidadeContratacao`: ENUM encontrado com 13 valores.
- ❌ **Situação da Contratação** → `situacaoCompraId`: ENUM não encontrado no manual.
- ❌ **Amparo Legal** → `amparoLegal`: ENUM não encontrado no manual.