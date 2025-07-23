# Tabelas de Domínio PNCP - Referências

Este diretório contém as tabelas de domínio oficiais extraídas do **Manual PNCP Consultas Versão 1**.

## Fonte Oficial
- **Documento**: Manual PNCP Consultas Versão 1
- **Localização**: `docs/openapi/MANUAL-PNCP-CONSULSTAS-VERSAO-1.md`
- **Data de Extração**: 2025-01-22

## Mapeamento de Arquivos para Seções do Manual

| Arquivo CSV | Seção Manual | Título da Seção | Linha no Manual |
|-------------|--------------|-----------------|-----------------|
| `call_instrument.csv` | 5.1 | Instrumento Convocatório | Linha 152 |
| `contracting_modality.csv` | 5.2 | Modalidade de Contratação | Linha 158 |
| `bidding_mode.csv` | 5.3 | Modo de Disputa | Linha 174 |
| `judgment_criteria.csv` | 5.4 | Critério de Julgamento | Linha 183 |
| `contracting_situation.csv` | 5.5 | Situação da Contratação | Linha 196 |
| `item_situation.csv` | 5.6 | Situação do Item da Contratação | Linha 205 |
| `benefit_type.csv` | 5.7 | Tipo de Benefício | Linha 215 |
| `result_situation.csv` | 5.8 | Situação do Resultado do Item da Contratação | Linha 225 |
| `contract_type.csv` | 5.9 | Tipo de Contrato | Linha 232 |
| `contract_term_type.csv` | 5.10 | Tipo de Termo de Contrato | Linha 249 |
| `process_category.csv` | 5.11 | Categoria do Processo | Linha 257 |

## Estrutura dos CSVs

Todos os arquivos CSV seguem a estrutura:
```csv
code,description
1,Descrição do código 1
2,Descrição do código 2
```

## Atualizações

Para atualizar estes dados:
1. Consulte o manual oficial mais recente
2. Compare com as seções correspondentes
3. Atualize os CSVs mantendo a estrutura `code,description`
4. Atualize este README com as mudanças

## Validação

Para validar os dados contra o manual oficial:
```bash
grep -n "### 5\." docs/openapi/MANUAL-PNCP-CONSULSTAS-VERSAO-1.md
```

Isso mostrará todas as seções de tabelas de domínio no manual.