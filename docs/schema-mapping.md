# Mapeamento de Campos: Schema Atual (Baliza) vs. Schema Oficial (PNCP)

## Análise

O schema atual do Baliza, definido em `dbt_baliza/models/bronze/bronze_pncp_source.yml`, utiliza uma arquitetura de tabelas divididas (`pncp_requests` e `pncp_content`) focada em deduplicação e metadados de extração, conforme o ADR-008. O conteúdo real da API PNCP é armazenado como um campo de texto (`response_content`) na tabela `pncp_content`.

Isso significa que não há um mapeamento direto de campos individuais no nível do banco de dados no *bronze layer*. O mapeamento ocorre em um *silver layer*, que ainda não está bem definido.

O schema oficial do PNCP, extraído do `MANUAL-PNCP-CONSULSTAS-VERSAO-1.md` e do `api-pncp-consulta.json`, é muito mais granular, com campos específicos para cada atributo das contratações, atas e contratos.

## Mapeamento de Conceitos

| Conceito Baliza (Atual) | Conceito PNCP (Oficial) | Análise e Ações Necessárias |
|---|---|---|
| Tabela `pncp_requests` | Metadados de cada consulta à API | A tabela `pncp_requests` armazena informações sobre as requisições (`endpoint_url`, `response_code`, etc.), o que é útil para monitoramento mas não faz parte do schema de dados de negócio do PNCP. **Ação: Manter para logging, mas não considerar como parte do schema de negócio.** |
| Tabela `pncp_content` | O payload de dados (`data`) retornado pela API | O campo `response_content` contém o JSON com a lista de contratações/atas/etc. Este campo precisa ser parseado e seus campos mapeados para um novo schema no *silver layer*. **Ação: Criar modelos no *silver layer* que parseiam `response_content` e o transformam em tabelas estruturadas.** |
| - | `Contratacao`, `Ata`, `Contrato` | O schema oficial é modelado em torno desses objetos de negócio. **Ação: Criar tabelas no *silver layer* que reflitam essa estrutura, como `silver_contratacoes`, `silver_atas`, `silver_contratos`.** |

## Campos Faltantes no Baliza (a serem criados no Silver Layer)

A partir do `response_content`, precisaremos criar colunas para **todos** os campos definidos no schema oficial. Exemplos para a entidade `Contratacao`:

- `numeroControlePNCP`
- `numeroCompra`
- `anoCompra`
- `processo`
- `modalidadeId` (usando o ENUM oficial)
- `situacaoCompraId` (usando o ENUM oficial)
- `objetoCompra`
- `valorTotalEstimado`
- `dataPublicacaoPncp`
- `orgaoEntidade.cnpj`
- `orgaoEntidade.razaoSocial`
- `unidadeOrgao.codigoUnidade`
- `unidadeOrgao.nomeUnidade`
- ... e todos os outros campos do manual.

## Campos Não-Oficiais no Baliza (a serem avaliados)

Os campos nas tabelas `pncp_requests` e `pncp_content` são focados na operação do extrator e não no negócio.

- `content_sha256`: Hash para deduplicação. **Manter no bronze layer.**
- `reference_count`: Contador para deduplicação. **Manter no bronze layer.**
- `first_seen_at`, `last_seen_at`: Metadados de observação. **Manter no bronze layer.**

Esses campos são úteis para a arquitetura de dados do Baliza, mas não devem ser propagados para o *gold layer* de analytics, a menos que sejam usados para análises de performance do extrator.

## Campos com Tipos Incorretos

Atualmente, tudo dentro de `response_content` é texto. A transformação no *silver layer* deve garantir a tipagem correta, conforme o schema OpenAPI.

| Campo Oficial | Tipo Oficial (OpenAPI) | Tipo no Baliza (Ação no Silver) |
|---|---|---|
| `valorTotalEstimado` | `number(decimal)` | `DECIMAL(18, 4)` |
| `anoCompra` | `integer` | `INTEGER` |
| `dataPublicacaoPncp` | `string(date-time)` | `TIMESTAMP` |
| `srp` | `boolean` | `BOOLEAN` |
| `modalidadeId` | `integer` | `INTEGER` (ou `ENUM` se o banco de dados suportar) |

## Breaking Changes Necessários

1.  **Criação de um Silver Layer Estruturado**: A mudança mais significativa será a introdução de modelos dbt que transformam o JSON bruto do `bronze.pncp_content` em tabelas estruturadas no `silver` layer.
2.  **Renomeação/Adaptação de Campos**: As queries que hoje consomem o JSON bruto precisarão ser reescritas para usar as novas tabelas do *silver layer*.
3.  **Adoção de ENUMs**: A implementação de ENUMs (ou tabelas de dimensão) para campos como `modalidadeId` e `situacaoCompraId` exigirá que as queries usem os códigos oficiais, não mais strings de texto.
4.  **Tipagem Forte**: As queries precisarão ser ajustadas para lidar com os tipos de dados corretos (e.g., `DECIMAL`, `TIMESTAMP`) em vez de apenas texto.

Este documento servirá como guia para a criação dos modelos do *silver layer* na Fase 3.
