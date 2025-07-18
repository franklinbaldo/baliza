> Este manualf oi convertido para markdown por LLM a partir do original em: https://www.gov.br/pncp/pt-br/central-de-conteudo/manuais/versoes-anteriores/ManualPNCPAPIConsultasVerso1.0.pdf


# Manual das APIs de Consultas PNCP

## Sumário

1. Objetivo
2. Protocolo de Comunicação
3. Acesso ao PNCP
    3.1. Endereços de Acesso
4. Recomendações Iniciais
    4.1. Composição do Número de Controle PNCP de PCA/Contratação/Ata/Contrato
        * Número de Controle do PCA
        * Número de Controle da Contratação
        * Número de Controle da Ata
        * Número de Controle do Contrato
    4.2. Dados de Retorno padronizados
5. Tabelas de Domínio
    5.1. Instrumento Convocatório
    5.2. Modalidade de Contratação
    5.3. Modo de Disputa
    5.4. Critério de Julgamento
    5.5. Situação da Contratação
    5.6. Situação do Item da Contratação
    5.7. Tipo de Benefício
    5.8. Situação do Resultado do Item da Contratação
    5.9. Tipo de Contrato
    5.10. Tipo de Termo de Contrato
    5.11. Categoria do Processo
    5.12. Tipo de Documento
    5.13. Natureza Jurídica
    5.14. Porte da Empresa
    5.15. Amparo Legal
    5.16. Categoria do Item do Plano de Contratações
    5.17. Identificador de Usuário
6. Catálogo de Serviços (APIs)
    6.1. Consultar Itens de PCA por Ano, idUsuario e Classificação Superior
    6.2. Consultar Itens de PCA por Ano e Classificação Superior
    6.3. Serviço Consultar Contratações por Data de Publicação
    6.4. Serviço Consultar Contratações com Período de Recebimento de Propostas em Aberto
    6.5. Serviço Consultar Atas de Registro de Preço por Período de Vigência
    6.6. Serviço Consultar Contratos por Data de Publicação
7. Suporte
8. Glossário

---

## 1. Objetivo

Este documento contempla as orientações para consultas aos dados de contratações, alienação de bens móveis e imóveis, atas de registro de preços e contratos realizados no âmbito da Lei nº 14.133/2021.

---

## 2. Protocolo de Comunicação

As consultas serão realizadas por meio de API (Application Programme Interface) que utiliza o protocolo de comunicação REST - Representational State Transfer/ HTTP 1.1 cujos dados trafegados utilizam a notação JSON - JavaScript Object Notation.

---

## 3. Acesso ao PNCP

### 3.1. Endereços de Acesso

A invocação dos serviços será realizada através das URLs citadas abaixo, conforme requisitos de segurança detalhados na seção seguinte.

**Ambiente de Produtivo**

*   **Portal:** https://pncp.gov.br
*   **Documentação Técnica (Serviços):** https://pncp.gov.br/api/consulta/swagger-ui/index.html
*   **Serviços (${BASE_URL}):** https://pncp.gov.br/api/consulta

*Nota: ${BASE_URL} será utilizada nos exemplos de requisições citados neste documento. É a URL base para acesso aos serviços disponíveis no PNCP.*

---

## 4. Recomendações Iniciais

### 4.1. Composição do Número de Controle PNCP de PCA/Contratação/Ata/Contrato

O PNCP gera automaticamente um identificador, que é um número de controle, no qual utiliza-se para reconhecer todas as demais transações realizadas para aquele registro.

Atualmente encontram-se disponíveis: plano de contratações anual (PCA), contratação (licitação ou contratação direta), ata de registro de preços ou contrato, conforme a composição abaixo:

#### Número de Controle do PCA

(id pca pncp) (Máscara: 99999999999999-0-999999/9999.)

Cada PCA receberá um número de controle composto por:

*   CNPJ do Órgão/Entidade do PCA (14 dígitos)
*   Dígito "0" - marcador que indica tratar-se de um plano de contratação anual
*   Número sequencial do Plano no PNCP\*
*   Ano do Plano (4 dígitos)

#### Número de Controle da Contratação

(id contratação pncp) (Máscara: 99999999999999-1-999999/9999.)

Cada contratação receberá um número de controle composto por:

*   CNPJ do Órgão/Entidade da contratação (14 dígitos)
*   Dígito "1" - marcador que indica tratar-se de uma contratação
*   Número sequencial da contratação no PNCP \*
*   Ano da contratação (4 dígitos)

#### Número de Controle da Ata

(id ata pncp) (Máscara: 99999999999999-1-999999/9999-999999.)

Cada ata receberá um número de controle composto por:

*   Número de Controle PNCP da Contratação (24 dígitos)
*   Número sequencial da ata no PNCP \*

#### Número de Controle do Contrato

(id contrato pncp) (Máscara: 99999999999999-2-999999/9999.)

Cada contrato receberá um número de controle composto por:

*   CNPJ do Órgão/Entidade do Contrato (14 dígitos)
*   Dígito "2" - marcador que indica tratar-se de um contrato
*   Número sequencial do contrato no PNCP \*
*   Ano do contrato (4 dígitos)

\*O número PNCP será gerado sequencialmente com 6 dígitos e reiniciado a cada mudança de ano.

### 4.2. Dados de Retorno padronizados

Ao realizar consultas para obter dados do Planos Anuais de Contratações – PCA e Contratações, a API realizará o procedimento de busca por esses dados e ao final será retornado o total de registros encontrados, o total de páginas necessárias para a obtenção de todos os registros, o número da página que a consulta foi realizada e o total de páginas restantes.

Essas informações são essenciais para tornar a entrega dos dados mais rápida possível, evitando demora ou até mesmo interrupção da entrega dos pacotes contendo os a informações solicitadas por parte do servidor de arquivos do PNCP.

**Dados de retorno**

| Id | Campo                    | Tipo    | Descrição                                                                                                   |
|----|--------------------------|---------|-------------------------------------------------------------------------------------------------------------|
| 1  | data                     | Vetor   | Vetor com os dados dos registros encontrados                                                                  |
| 2  | totalRegistros           | Inteiro | Total de registros encontrados                                                                                |
| 3  | totalPaginas             | Inteiro | Total de páginas necessárias para a obtenção de todos os registros                                             |
| 4  | numeroPagina             | Inteiro | Número da página que a consulta foi realizada                                                               |
| 5  | paginasRestantes         | Inteiro | Total de páginas restantes                                                                                  |
| 6  | empty                    | Boleano | Indicador se o atributo data está vazio                                                                     |

---

## 5. Tabelas de Domínio

A seguir são encontradas informações sobre as tabelas de domínio, ou seja, listas dados de interesse que contem valores fixos, usados em várias consultas que tem o intuito de auxiliar na realização e consultas.

### 5.1. Instrumento Convocatório

*   (código = 1) Edital: Instrumento convocatório utilizado no diálogo competitivo, concurso, concorrência, pregão, manifestação de interesse, pré-qualificação e credenciamento.
*   (código = 2) Aviso de Contratação Direta: Instrumento convocatório utilizado na Dispensa com Disputa.
*   (código = 3) Ato que autoriza a Contratação Direta: Instrumento convocatório utilizado na Dispensa sem Disputa ou na Inexigibilidade.

### 5.2. Modalidade de Contratação

*   (código = 1) Leilão - Eletrônico
*   (código = 2) Diálogo Competitivo
*   (código = 3) Concurso
*   (código = 4) Concorrência - Eletrônica
*   (código = 5) Concorrência - Presencial
*   (código = 6) Pregão - Eletrônico
*   (código = 7) Pregão - Presencial
*   (código = 8) Dispensa de Licitação
*   (código = 9) Inexigibilidade
*   (código = 10) Manifestação de Interesse
*   (código = 11) Pré-qualificação
*   (código = 12) Credenciamento
*   (código = 13) Leilão - Presencial

### 5.3. Modo de Disputa

*   (código = 1) Aberto
*   (código = 2) Fechado
*   (código = 3) Aberto-Fechado
*   (código = 4) Dispensa Com Disputa
*   (código = 5) Não se aplica
*   (código = 6) Fechado-Aberto

### 5.4. Critério de Julgamento

*   (código = 1) Menor preço
*   (código = 2) Maior desconto
*   (código = 4) Técnica e preço
*   (código = 5) Maior lance
*   (código = 6) Maior retorno econômico
*   (código = 7) Não se aplica
*   (código = 8) Melhor técnica
*   (código = 9) Conteúdo artístico

---

## 5.5. Situação da Contratação

*   (código = 1) Divulgada no PNCP: Contratação divulgada no PNCP. Situação atribuída na inclusão da contratação.
*   (código = 2) Revogada: Contratação revogada conforme justificativa.
*   (código = 3) Anulada: Contratação revogada conforme justificativa.
*   (código = 4) Suspensa: Contratação suspensa conforme justificativa.

---

## 5.6. Situação do Item da Contratação

*   (código = 1) Em Andamento: Item com disputa/seleção do fornecedor/arrematante não finalizada. Situação atribuída na inclusão do item da contratação
*   (código = 2) Homologado: Item com resultado (fornecedor/arrematante informado)
*   (código = 3) Anulado/Revogado/Cancelado: Item cancelado conforme justificativa
*   (código = 4) Deserto: Item sem resultado (sem fornecedores/arrematantes interessados)
*   (código = 5) Fracassado: Item sem resultado (fornecedores/arrematantes desclassificados ou inabilitados)

---

## 5.7. Tipo de Benefício

*   (código = 1) Participação exclusiva para ME/EPP
*   (código = 2) Subcontratação para ME/EPP
*   (código = 3) Cota reservada para ME/EPP
*   (código = 4) Sem benefício
*   (código = 5) Não se aplica

---

## 5.8. Situação do Resultado do Item da Contratação

*   (código = 1) Informado: Que possui valor e fornecedor e marca oriundo do resultado da contratação. Situação atribuída na inclusão do resultado do item da contratação.
*   (código = 2) Cancelado: Resultado do item cancelado conforme justificativa.

---

## 5.9. Tipo de Contrato

*   (código = 1) Contrato (termo inicial): Acordo formal recíproco de vontades firmado entre as partes
*   (código = 2) Comodato: Contrato de concessão de uso gratuito de bem móvel ou imóvel
*   (código = 3) Arrendamento: Contrato de cessão de um bem por um determinado período mediante pagamento
*   (código = 4) Concessão: Contrato firmado com empresa privada para execução de serviço público sendo remunerada por tarifa
*   (código = 5) Termo de Adesão: Contrato em que uma das partes estipula todas as cláusulas sem a outra parte poder modificá-las
*   (código = 6) Convênio: Acordos firmados entre as partes buscando a realização de um objetivo em comum
*   (código = 7) Empenho: É uma promessa de pagamento por parte do Estado para um fim específico
*   (código = 8) Outros: Outros tipos de contratos que não os listados
*   (código = 9) Termo de Execução Descentralizada (TED): Instrumento utilizado para a descentralização de crédito entre órgãos/entidades da União
*   (código = 10) Acordo de Cooperação Técnica (ACT): Acordos firmados entre órgãos visando a execução de programas de trabalho ou projetos
*   (código = 11) Termo de Compromisso: Acordo firmado para cumprir compromisso estabelecido entre as partes
*   (código = 12) Carta Contrato: Documento que formaliza e ratifica acordo entre duas ou mais partes nas hipóteses em que a lei dispensa a celebração de um contrato

---

## 5.10. Tipo de Termo de Contrato

*   (código = 1) Termo de Rescisão: Encerramento é antes da data final do contrato.
*   (código = 2) Termo Aditivo: Atualiza o contrato como um todo, podendo prorrogar, reajustar, acrescer, suprimir, alterar cláusulas e reajustar.
*   (código = 3) Termo de Apostilamento: Atualiza o valor do contrato.

---

## 5.11. Categoria do Processo

*   (código = 1) Cessão
*   (código = 2) Compras
*   (código = 3) Informática (TIC)
*   (código = 4) Internacional
*   (código = 5) Locação Imóveis
*   (código = 6) Mão de Obra
*   (código = 7) Obras
*   (código = 8) Serviços
*   (código = 9) Serviços de Engenharia
*   (código = 10) Serviços de Saúde
*   (código = 11) Alienação de bens móveis/imóveis

---

## 5.12. Tipo de Documento

**Tipos de documentos da contratação:**

*   (código = 1) Aviso de Contratação Direta
*   (código = 2) Edital
*   **Outros anexos:**
    *   (código = 3) Minuta do Contrato
    *   (código = 4) Termo de Referência
    *   (código = 5) Anteprojeto
    *   (código = 6) Projeto Básico
    *   (código = 7) Estudo Técnico Preliminar
    *   (código = 8) Projeto Executivo
    *   (código = 9) Mapa de Riscos
    *   (código = 10) DFD

**Tipos de documentos da ata de registro de preço:**

*   (código = 11) Ata de Registro de Preço

**Tipos de documentos de contrato:**

*   (código = 12) Contrato
*   (código = 13) Termo de Rescisão
*   (código = 14) Termo Aditivo
*   (código = 15) Termo de Apostilamento
*   (código = 17) Nota de Empenho
*   (código = 18) Relatório Final de Contrato

\*\* Para outros documentos do processo usar o código 16.

---

## 5.13. Natureza Jurídica

**Código - Natureza jurídica**

*   0000 - Natureza Jurídica não informada
*   1015 - Órgão Público do Poder Executivo Federal
*   1023 - Órgão Público do Poder Executivo Estadual ou do Distrito Federal
*   1031 - Órgão Público do Poder Executivo Municipal
*   1040 - Órgão Público do Poder Legislativo Federal
*   1058 - Órgão Público do Poder Legislativo Estadual ou do Distrito Federal
*   1066 - Órgão Público do Poder Legislativo Municipal
*   1074 - Órgão Público do Poder Judiciário Federal
*   1082 - Órgão Público do Poder Judiciário Estadual
*   1104 - Autarquia Federal
*   1112 - Autarquia Estadual ou do Distrito Federal
*   1120 - Autarquia Municipal
*   1139 - Fundação Pública de Direito Público Federal
*   1147 - Fundação Pública de Direito Público Estadual ou do Distrito Federal
*   1155 - Fundação Pública de Direito Público Municipal
*   1163 - Órgão Público Autônomo Federal
*   1171 - Órgão Público Autônomo Estadual ou do Distrito Federal
*   1180 - Órgão Público Autônomo Municipal
*   1198 - Comissão Polinacional
*   1210 - Consórcio Público de Direito Público (Associação Pública)
*   1228 - Consórcio Público de Direito Privado
*   1236 - Estado ou Distrito Federal
*   1244 - Município
*   1252 - Fundação Pública de Direito Privado Federal
*   1260 - Fundação Pública de Direito Privado Estadual ou do Distrito Federal
*   1279 - Fundação Pública de Direito Privado Municipal
*   1287 - Fundo Público da Administração Indireta Federal
*   1295 - Fundo Público da Administração Indireta Estadual ou do Distrito Federal
*   1309 - Fundo Público da Administração Indireta Municipal
*   1317 - Fundo Público da Administração Direta Federal
*   1325 - Fundo Público da Administração Direta Estadual ou do Distrito Federal
*   1333 - Fundo Público da Administração Direta Municipal
*   1341 - União
*   2011 - Empresa Pública
*   2038 - Sociedade de Economia Mista
*   2046 - Sociedade Anônima Aberta
*   2054 - Sociedade Anônima Fechada

---

## 5.14. Porte da Empresa

*   (código = 1) ME: Microempresa
*   (código = 2) EPP: Empresa de pequeno porte
*   (código = 3) Demais: Demais empresas
*   (código = 4) Não se aplica: Quando o fornecedor/arrematante for pessoa física.
*   (código = 5) Não informado: Quando não possuir o porte da empresa.

---

## 5.15. Amparo Legal

*   (código = 1) Lei 14.133/2021, Art. 28, I
*   (código = 2) Lei 14.133/2021, Art. 28, II
*   (código = 3) Lei 14.133/2021, Art. 28, III
*   (código = 4) Lei 14.133/2021, Art. 28, IV
*   (código = 5) Lei 14.133/2021, Art. 28, V
*   (código = 6) Lei 14.133/2021, Art. 74, I
*   (código = 7) Lei 14.133/2021, Art. 74, II
*   (código = 8) Lei 14.133/2021, Art. 74, III, a
*   (código = 9) Lei 14.133/2021, Art. 74, III, b
*   (código = 10) Lei 14.133/2021, Art. 74, III, c
*   (código = 11) Lei 14.133/2021, Art. 74, III, d
*   (código = 12) Lei 14.133/2021, Art. 74, III, e
*   (código = 13) Lei 14.133/2021, Art. 74, III, f
*   (código = 14) Lei 14.133/2021, Art. 74, III, g
*   (código = 15) Lei 14.133/2021, Art. 74, III, h
*   (código = 16) Lei 14.133/2021, Art. 74, IV
*   (código = 17) Lei 14.133/2021, Art. 74, V
*   (código = 18) Lei 14.133/2021, Art. 75, I
*   (código = 19) Lei 14.133/2021, Art. 75, II
*   (código = 20) Lei 14.133/2021, Art. 75, III, a
*   (código = 21) Lei 14.133/2021, Art. 75, III, b
*   (código = 22) Lei 14.133/2021, Art. 75, IV, a
*   (código = 23) Lei 14.133/2021, Art. 75, IV, b
*   (código = 24) Lei 14.133/2021, Art. 75, IV, c
*   (código = 25) Lei 14.133/2021, Art. 75, IV, d
*   (código = 26) Lei 14.133/2021, Art. 75, IV, e
*   (código = 27) Lei 14.133/2021, Art. 75, IV, f
*   (código = 28) Lei 14.133/2021, Art. 75, IV, g

---

## 5.16. Categoria do Item do Plano de Contratações

*   (código = 1) Material
*   (código = 2) Serviço
*   (código = 3) Obras
*   (código = 4) Serviços de Engenharia
*   (código = 5) Soluções de TIC
*   (código = 6) Locação de Imóveis
*   (código = 7) Alienação/Concessão/Permissão
*   (código = 8) Obras e Serviços de Engenharia

---

## 5.17. Identificador de Usuário

Para uso de algumas APIs pode ser necessário incluir o Identificador Único do portal ou sistema integrado (idUsuario). Essa informação pode ser encontrada acessando o sítio: Portais Integrados ao PNCP - Portal Nacional de Contratações Públicas - PNCP (www.gov.br) e clicando em “Pesquisa ID” conforme imagem a seguir:

*(Imagem do portal PNCP, mostrando a opção "Pesquisa ID")*

---

## 6. Catálogo de Serviços (APIs)

### 6.1. Consultar Itens de PCA por Ano, idUsuario e Classificação Superior

Serviço que permite recuperar a lista de itens pertencentes a um determinado Plano de Contratações Anual (PCA) por determinado ano e usuário (Portais de Contratações), opcionalmente filtrando por ordem de classificação superior.

**Detalhes de Requisição**

| Endpoint             | Método HTTP | Exemplo de Payload |
|----------------------|-------------|--------------------|
| /v1/pca/usuario      | GET         | Não se aplica      |

**Exemplo Requisição (cURL)**

```bash
curl -X 'GET' \
  'https://pncp.gov.br/api/consulta/v1/pca/usuario?anoPca=2023&idUsuario=3&codigoClassificacaoSuperior=979&pagina=1' \
  -H 'accept: */*'
```

**Dados de entrada**

*Nota: Alimentar o parâmetro {anoPca}, {idUsuario} e {pagina} na URL.*

| Campo                         | Tipo        | Obrigatório | Descrição                                                                                                                                              |
|-------------------------------|-------------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| anoPca                        | Inteiro     | Sim         | Ano do PCA                                                                                                                                             |
| idUsuario                     | Inteiro     | Sim         | Número de identificação do usuário (Sistema de Contratações Públicas) que publicou a informação no Portal PNCP                                       |
| codigoClassificacaoSuperior   | Texto (100) | Não         | Código da Classe do material ou Grupo do serviço conforme catálogos de matérias e serviços utilizados pelos portais de compras.                          |
| pagina                        | Inteiro     | Sim         | Número da página que se deseja obter os dados.                                                                                                         |
| tamanhoPagina                 | Inteiro     | Não         | Por padrão cada página contém no máximo 500 registros, no entanto o tamanho de registros em cada página pode ser ajustado (até o limite de 500 registros) com vistas a tornar a entrega de dados mais rápida. |

**Dados de retorno**

| Id | Campo                         | Tipo    | Descrição                                                                |
|----|-------------------------------|---------|--------------------------------------------------------------------------|
| 1  | orgaoEntidadeCnpj             | Texto   | CNPJ do Órgão pertencente ao PCA                                         |
| 2  | orgaoEntidadeRazaoSocial      | Texto   | Razão Social do Órgão pertencente ao PCA                                 |
| 3  | codigoUnidade                 | Texto   | Código da Unidade Responsável do Órgão                                   |
| 4  | nomeUnidade                   | Texto   | Nome da Unidade Responsável                                              |
| 5  | anoPca                        | Inteiro | Ano do Plano de Contratações da Unidade                                  |
| 6  | idPcaPncp                     | Texto   | Número de Controle PNCP do PCA (id PCA PNCP)                             |
| 7  | dataPublicacaoPncp            | Data    | Data da publicação do item do plano no PNCP                               |
| 8  | Lista                         | Lista   | Lista de Itens do PCA da Unidade                                         |
| 8.1| numeroltem                    | Inteiro | Número do item no Plano (único e sequencial crescente)                 |
| 8.2| categorialtemPcaNome          | Texto   | Nome categoria do item conforme tabela de domínio Categoria do Item do Plano de Contratações |
| 8.3| classificacaoCatalogold      | Texto   | Código da Indicação se Item é Material ou Serviço. Domínio: 1 - Material; 2 - Serviço; |
| 8.4| nomeClassificacaoCatalogo     | Texto   | Nome da Indicação se Item é Material ou Serviço. Domínio: 1 - Material; 2 - Serviço; |
| 8.5| classificacaoSuperiorCodigo   | Texto (100) | Código da Classe do material ou Grupo do serviço conforme catálogo           |
| 8.6| classificacaoSuperiorNome     | Texto (255) | Descrição da Classe do material ou Grupo do serviço conforme catálogo      |
| 8.7| pdmCodigo                     | Texto (100) | Código PDM referente ao material conforme o CNBS                         |
| 8.8| pdmDescricao                  | Texto (255) | Descrição PDM referente ao material conforme o CNBS                      |
| 8.9| codigoltem                    | Texto (100) | Código do Material ou Serviço conforme o catálogo utilizado              |
| 8.10| descricaoltem                 | Texto (2048)| Descrição do material ou serviço conforme catálogo utilizado             |
| 8.11| unidadeFornecimento           | Texto   | Unidade de fornecimento                                                  |
| 8.12| quantidadeEstimada            | Decimal | Quantidade estimada do item do plano de contratação (maior ou igual a zero). Precisão de até 4 dígitos decimais; Ex: 10.0001; |
| 8.13| valorUnitario                 | Decimal | Valor unitário do item (maior ou igual a zero). Precisão de até 4 dígitos decimais; Ex: 100.0001; |
| 8.14| valorTotal                    | Decimal | Valor total do item (maior ou igual a zero). Precisão de até 4 dígitos decimais; Ex: 100.0001; |
| 8.15| valorOrcamentoExercicio       | Decimal | Valor orçamentário estimado para o exercício (maior ou igual a zero). Precisão de até 4 dígitos decimais; Ex: 100.0001; |
| 8.16| dataDesejada                  | Data    | Data desejada para a contratação                                         |
| 8.17| unidadeRequisitante           | Texto   | Nome da unidade requisitante                                             |
| 8.18| grupoContratacaoCodigo        | Texto   | Código da Contratação Futura                                             |
| 8.19| grupoContratacaoNome          | Texto   | Nome da Contratação Futura                                               |
| 8.20| datalnclusao                  | Data    | Data da inclusão do registro do item do plano no PNCP                    |
| 8.21| dataAtualizacao               | Data    | Data da última atualização do registro do item do plano                 |

**Códigos de Retorno**

| Código HTTP | Mensagem    | Tipo    |
|-------------|-------------|---------|
| 200         | OK          | Sucesso |
| 204         | No Content  | Sucesso |
| 400         | Bad Request | Erro    |
| 422         | Unprocessable Entity | Erro    |
| 500         | Internal Server Error | Erro    |

---

## 6.2. Consultar Itens de PCA por Ano e Classificação Superior

Serviço que permite recuperar a lista de itens pertencentes a um determinado Plano de Contratações Anual (PCA), opcionalmente filtrando por ordem de classificação superior.

**Detalhes de Requisição**

| Endpoint    | Método HTTP | Exemplo de Payload |
|-------------|-------------|--------------------|
| /v1/pca/    | GET         | Não se aplica      |

**Exemplo Requisição (cURL)**

```bash
curl -X 'GET' \
  'https://pncp.gov.br/api/consulta/v1/pca/?anoPca=2023&codigoClassificacaoSuperior=979&pagina=1' \
  -H 'accept: */*'
```

**Dados de entrada**

*Nota: Alimentar o parâmetro {ano} na URL.*

| Campo                         | Tipo        | Obrigatório | Descrição                                                                                                       |
|-------------------------------|-------------|-------------|-----------------------------------------------------------------------------------------------------------------|
| anoPca                        | Inteiro     | Sim         | Ano do PCA                                                                                                      |
| codigoClassificacaoSuperior   | Texto (100) | Sim         | Código da Classe do material ou Grupo do serviço conforme catálogos de matérias e serviços utilizados pelos portais de compras. |
| pagina                        | Inteiro     | Sim         | Número da página que se deseja obter os dados.                                                                  |
| tamanhoPagina                 | Inteiro     | Não         | Por padrão cada página contém no máximo 500 registros, no entanto o tamanho de registros em cada página pode ser ajustado (até o limite de 500 registros) com vistas a tornar a entrega de dados mais rápida. |

**Dados de retorno**

| Id | Campo                         | Tipo    | Descrição                                                                |
|----|-------------------------------|---------|--------------------------------------------------------------------------|
| 1  | orgaoEntidadeCnpj             | Texto   | CNPJ do Órgão                                                            |
| 2  | orgaoEntidadeRazaoSocial      | Texto   | Razão Social do Órgão                                                    |
| 3  | codigoUnidade                 | Texto   | Código da Unidade Responsável                                            |
| 4  | nomeUnidade                   | Texto   | Nome da Unidade Responsável                                              |
| 5  | anoPca                        | Inteiro | Ano do Plano de Contratações da Unidade                                  |
| 6  | idPcaPncp                     | Texto   | Número de Controle PNCP do PCA (id PCA PNCP)                             |
| 7  | dataPublicacaoPncp            | Data    | Data da publicação do item do plano no PNCP                               |
| 8  | Lista                         | Lista   | Lista de Itens do PCA da Unidade                                         |
| 8.1| numeroltem                    | Inteiro | Número do item no Plano (único e sequencial crescente)                 |
| 8.2| categorialtemPcaNome          | Texto   | Nome categoria do item conforme tabela de domínio Categoria do Item do Plano de Contratações |
| 8.3| classificacaoCatalogold      | Texto   | Código da Indicação se Item é Material ou Serviço. Domínio: 1 - Material; 2 - Serviço; |
| 8.4| nomeClassificacaoCatalogo     | Texto   | Nome da Indicação se Item é Material ou Serviço. Domínio: 1 - Material; 2 - Serviço; |
| 8.5| classificacaoSuperiorCodigo   | Texto (100) | Código da Classe do material ou Grupo do serviço conforme catálogo           |
| 8.6| classificacaoSuperiorNome     | Texto (255) | Descrição da Classe do material ou Grupo do serviço conforme catálogo      |
| 8.7| pdmCodigo                     | Texto (100) | Código PDM referente ao material conforme o CNBS                         |
| 8.8| pdmDescricao                  | Texto (255) | Descrição PDM referente ao material conforme o CNBS                      |
| 8.9| codigoltem                    | Texto (100) | Código do Material ou Serviço conforme o catálogo utilizado              |
| 8.10| descricaoltem                 | Texto (2048)| Descrição do material ou serviço conforme catálogo utilizado             |
| 8.11| unidadeFornecimento           | Texto   | Unidade de fornecimento                                                  |
| 8.12| quantidadeEstimada            | Decimal | Quantidade estimada do item do plano de contratação (maior ou igual a zero). Precisão de até 4 dígitos decimais; Ex: 10.0001; |
| 8.13| valorUnitario                 | Decimal | Valor unitário do item (maior ou igual a zero). Precisão de até 4 dígitos decimais; Ex: 100.0001; |
| 8.14| valorTotal                    | Decimal | Valor total do item (maior ou igual a zero). Precisão de até 4 dígitos decimais; Ex: 100.0001; |
| 8.15| valorOrcamentoExercicio       | Decimal | Valor orçamentário estimado para o exercício (maior ou igual a zero). Precisão de até 4 dígitos decimais; Ex: 100.0001; |
| 8.16| dataDesejada                  | Data    | Data desejada para a contratação                                         |
| 8.17| unidadeRequisitante           | Texto   | Nome da unidade requisitante                                             |
| 8.18| grupoContratacaoCodigo        | Texto   | Código da Contratação Futura                                             |
| 8.19| grupoContratacaoNome          | Texto   | Nome da Contratação Futura                                               |
| 8.20| datalnclusao                  | Data    | Data da inclusão do registro do item do plano no PNCP                    |
| 8.21| dataAtualizacao               | Data    | Data da última atualização do registro do item do plano                 |

---

## 6.3. Serviço Consultar Contratações por Data de Publicação

Serviço que permite consultar contratações publicadas no PNCP por um período informado. Junto à data inicial e data final informadas deverá ser informado o código da Modalidade da Contratação (vide tabela XXX). Opcionalmente poderá ser informado código do Modo de Disputa da Contratação (vide tabela XXX), código do IBGE do Município, sigla da Unidade Federativa da Unidade Administrativa do Órgão, CNPJ do Órgão/Entidade, código da Unidade Administrativa do Órgão/Entidade ou código de identificação do Usuário (Sistema de Contratações Públicas que publicou a Contratação) para refinar a consulta.

**Detalhes de Requisição**

| Endpoint                     | Método HTTP | Exemplo de Payload |
|------------------------------|-------------|--------------------|
| /v1/contratacoes/publicacao | GET         | Não se aplica      |

**Exemplo Requisição (cURL)**

```bash
curl -X 'GET' \
  'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?dataInicial=20230801&dataFinal=20230802&codigoModalidadeContratacao=8&uf=DF&codigoMunicipiolbge=5300108&cnpj=00059311000126&codigoUnidadeAdministrativa=194035&idUsuario=3&pagina=1' \
  -H 'accept: */*'
```

**Dados de entrada**

*Nota: Dados a serem enviados no cabeçalho da requisição.*

| Campo                         | Tipo    | Obrigatório | Descrição                                                                                                                      |
|-------------------------------|---------|-------------|--------------------------------------------------------------------------------------------------------------------------------|
| dataInicial                   | Data    | Sim         | Data inicial do período a ser consultado no formato AAAAMMDD.                                                                |
| dataFinal                     | Data    | Sim         | Data final do período a ser consultado no formato AAAAMMDD.                                                                    |
| codigoModalidadeContratacao   | Inteiro | Sim         | Código da tabela de domínio referente a Modalidade da Contratação                                                            |
| codigoModoDisputa             | Inteiro | Não         | Código da tabela de domínio referente a Modo de Disputa                                                                        |
| uf                            | String  | Não         | Sigla da Unidade Federativa referente à Unidade Administrativa do órgão                                                        |
| codigoMunicipiolbge           | String  | Não         | Código IBGE do Município da Unidade Administrativa                                                                             |
| cnpj                          | String  | Não         | Cnpj do órgão originário da contratação informado na inclusão (proprietário da contratação)                                  |
| codigoUnidadeAdministrativa   | String  | Não         | Código da Unidade Administrativa do Órgão originário da contratação informado na inclusão (proprietário da contratação)       |
| idUsuario                     | Inteiro | Não         | Identificado do sistema usuário (Sistema de Contratações Públicas) que publicou a contratação.                               |
| pagina                        | Inteiro | Sim         | Número da página que se deseja obter os dados.                                                                                 |
| tamanhoPagina                 | Inteiro | Não         | Por padrão cada página contém no máximo 500 registros, no entanto o tamanho de registros em cada página pode ser ajustado (até o limite de 500 registros) com vistas a tornar a entrega de dados mais rápida. |

**Dados de retorno**

| Id | Campo                         | Tipo      | Descrição                                                                                                          |
|----|-------------------------------|-----------|--------------------------------------------------------------------------------------------------------------------|
| 1  | numeroControlePNCP            | String    | Número de Controle PNCP da Contratação (id Contratação PNCP)                                                       |
| 2  | numeroCompra                  | Texto (50)| Número da Contratação no sistema de origem                                                                       |
| 3  | anoCompra                     | Inteiro   | Ano da Contratação                                                                                                 |
| 4  | processo                      | Texto (50)| Número do processo de Contratação no sistema de origem                                                             |
| 5  | tipolnstrumentoConvocatoriold | Inteiro   | Código do instrumento convocatório da Contratação                                                                  |
| 6  | tipolnstrumentoConvocatorioNome | String  | Nome do instrumento convocatório da Contratação                                                                    |
| 7  | modalidadeld                  | Inteiro   | Código da Modalidade referente à Contratação                                                                       |
| 8  | modalidadeNome                | String    | Modalidade referente à Contratação                                                                                 |
| 9  | modoDisputald                 | Inteiro   | Código do modo de disputa referente à Contratação                                                                  |
| 10 | modoDisputaNome               | String    | Modo de disputa referente à Contratação                                                                          |
| 11 | situacaoComprald              | Inteiro   | Código da situação da Contratação                                                                                  |
| 12 | situacaoCompraNome            | Inteiro   | Situação da Contratação                                                                                            |
| 13 | objetoCompra                  | Texto (5120)| Descrição do Objeto referente à Contratação                                                                      |
| 14 | informacaoComplementar        | Texto (5120)| Informação Complementar do objeto referente à Contratação                                                          |
| 15 | srp                           | Boleano   | Identifica se a compra trata-se de um SRP (Sistema de registro de preços)                                          |
| 16 | amparoLegal                   |           | Dados do amparo legal                                                                                              |
| 16.1| codigo                        | Inteiro   | Código do Amparo Legal                                                                                             |
| 16.2| nome                          | String    | Nome do Amparo Legal                                                                                               |
| 16.3| descricao                     | String    | Descrição do Amparo legal                                                                                          |
| 17 | valorTotalEstimado            | Decimal   | Valor total estimado da Contratação. Precisão de até 4 dígitos decimais; Ex: 100.0001; Obs: Retornará valor zero (0) se atributo orcamentoSigiloso for true e o item não possuir resultado. |
| 18 | valorTotalHomologado          | Decimal   | Valor total homologado com base nos resultados incluídos. Precisão de até 4 dígitos decimais; Ex: 100.0001;          |
| 19 | dataAberturaProposta          | Data e Hora | Data de abertura do recebimento de propostas (horário de Brasília)                                                 |
| 20 | dataEncerramentoProposta      | Data e Hora | Data de encerramento do recebimento de propostas (horário de Brasília)                                             |
| 21 | dataPublicacaoPncp            | Data      | Data da publicação da Contratação no PNCP                                                                         |
| 22 | datalnclusao                  | Data      | Data da inclusão do registro da Contratação no PNCP                                                                |
| 23 | dataAtualizacao               | Data      | Data da última atualização do registro da Contratação                                                              |
| 24 | sequencialCompra              | Inteiro   | Sequencial da Contratação no PNCP; Número sequencial gerado no momento que a contratação foi inserida no PNCP;    |
| 25 | orgaoEntidade                 |           | Dados do Órgão/Entidade                                                                                            |
| 25.1| cnpj                          | String    | CNPJ do Órgão referente à Contratação                                                                            |
| 25.2| razaosocial                   | String    | Razão social do Órgão referente à Contratação                                                                    |
| 25.3| poderld                       | String    | Código do poder a que pertence o Órgão. L - Legislativo; E - Executivo; J - Judiciário                           |
| 25.4| esferald                      | String    | Código da esfera a que pertence o Órgão. F - Federal; E - Estadual; M - Municipal; D - Distrital                  |
| 26 | unidadeOrgao                  |           | Dados da Unidade Administrativa                                                                                    |
| 26.1| codigoUnidade                 | String    | Código da Unidade Administrativa pertencente ao Órgão                                                             |
| 26.2| nomeUnidade                   | String    | Nome da Unidade Administrativa pertencente ao Órgão                                                               |
| 26.3| codigolbge                    | Inteiro   | Código IBGE do município                                                                                           |
| 26.4| municipioNome                 | String    | Nome do município                                                                                                  |
| 26.5| ufSigla                       | String    | Sigla da unidade federativa do município                                                                           |
| 26.6| ufNome                        | String    | Nome da unidade federativa do município                                                                            |
| 27 | orgaoSubRogado                |           | Dados do Órgão/Entidade subrogado                                                                                  |
| 28.1| cnpj                          | String    | CNPJ do Órgão referente à Contratação                                                                            |
| 28.2| razaosocial                   | String    | Razão social do Órgão referente à Contratação                                                                    |
| 28.3| poderld                       | String    | Código do poder a que pertence o Órgão. L - Legislativo; E - Executivo; J - Judiciário                           |
| 28.4| esferald                      | String    | Código da esfera a que pertence o Órgão. F - Federal; E - Estadual; M - Municipal; D - Distrital                  |
| 29 | unidadeSubRogada              |           | Dados da Unidade Administrativa do Órgão subrogado                                                                 |
| 29.1| codigoUnidade                 | String    | Código da Unidade Administrativa pertencente ao Órgão subrogado                                                   |
| 29.2| nomeUnidade                   | String    | Nome da Unidade Administrativa pertencente ao Órgão subrogado                                                     |
| 29.3| codigolbge                    | Inteiro   | Código IBGE do município                                                                                           |
| 29.4| municipioNome                 | String    | Nome do município                                                                                                  |
| 29.5| ufSigla                       | String    | Sigla da unidade federativa do município                                                                           |
| 29.6| ufNome                        | String    | Nome da unidade federativa do município                                                                            |
| 30 | usuarioNome                   | String    | Nome do Usuário/Sistema que enviou a Contratação                                                                 |
| 31 | linkSistemaOrigem             | String    | URL para página/portal do sistema de origem da contratação para recebimento de propostas.                          |
| 32 | justificativaPresencial       | String    | Justificativa pela escolha da modalidade presencial.                                                               |

**Códigos de Retorno**

| Código HTTP | Mensagem             | Tipo    |
|-------------|----------------------|---------|
| 200         | OK                   | Sucesso |
| 204         | No Content           | Sucesso |
| 400         | Bad Request          | Erro    |
| 422         | Unprocessable Entity | Erro    |
| 500         | Internal Server Error| Erro    |

---

## 6.4. Serviço Consultar Contratações com Período de Recebimento de Propostas em Aberto

Serviço que permite consultar contratações publicadas no PNCP por um período informado. Opcionalmente poderá ser informado o código da Modalidade da Contratação código do IBGE do Município, sigla da Unidade Federativa da Unidade Administrativa do Órgão, CNPJ do Órgão/Entidade, código da Unidade Administrativa do Órgão/Entidade ou código de identificação do Usuário (Sistema de Contratações Públicas que publicou a Contratação) para refinar a consulta.

**Detalhes de Requisição**

| Endpoint                 | Método HTTP | Exemplo de Payload |
|--------------------------|-------------|--------------------|
| /v1/contratacoes/proposta | GET         | Não se aplica      |

**Exemplo Requisição (cURL)**

```bash
curl -k -X 'GET' \
  "${BASE_URL}/v1/contratacoes/proposta?dataFinal=20230831&codigoModalidadeContratacao=8&pagina=1" \
  -H "accept: */*"
```

**Dados de entrada**

*Nota: Dados a serem enviados no cabeçalho da requisição.*

| Campo                         | Tipo    | Obrigatório | Descrição                                                                                                       |
|-------------------------------|---------|-------------|-----------------------------------------------------------------------------------------------------------------|
| dataFinal                     | Data    | Sim         | Data final do período a ser consultado no formato AAAAMMDD.                                                     |
| codigoModalidadeContratacao   | Inteiro | Sim         | Código da tabela de domínio Modalidade da Contratação                                                           |
| uf                            | String  | Não         | Sigla da Unidade Federativa referente à Unidade Administrativa do órgão                                         |
| codigoMunicipiolbge           | String  | Não         | Código IBGE do Município da Unidade Administrativa                                                              |
| cnpj                          | String  | Não         | Cnpj do órgão originário da contratação informado na inclusão (proprietário da contratação)                   |
| codigoUnidadeAdministrativa   | String  | Não         | Código da Unidade Administrativa do Órgão originário da contratação informado na inclusão (proprietário da contratação) |
| idUsuario                     | Inteiro | Não         | Identificado do sistema usuário (Sistema de Contratações Públicas) que publicou a contratação.                 |
| pagina                        | Inteiro | Sim         | Número da página que se deseja obter os dados.                                                                  |
| tamanhoPagina                 | Inteiro | Não         | Por padrão cada página contém no máximo 500 registros, no entanto o tamanho de registros em cada página pode ser ajustado (até o limite de 500 registros) com vistas a tornar a entrega de dados mais rápida. |

**Dados de retorno**

| Id | Campo                         | Tipo      | Descrição                                                                                                          |
|----|-------------------------------|-----------|--------------------------------------------------------------------------------------------------------------------|
| 1  | numeroControlePNCP            | String    | Número de Controle PNCP da Contratação (id Contratação PNCP)                                                       |
| 2  | numeroCompra                  | Texto (50)| Número da Contratação no sistema de origem                                                                       |
| 3  | anoCompra                     | Inteiro   | Ano da Contratação                                                                                                 |
| 4  | processo                      | Texto (50)| Número do processo de Contratação no sistema de origem                                                             |
| 5  | tipolnstrumentoConvocatoriold | Inteiro   | Código do instrumento convocatório da Contratação                                                                  |
| 6  | tipolnstrumentoConvocatorioNome | String  | Nome do instrumento convocatório da Contratação                                                                    |
| 7  | modalidadeld                  | Inteiro   | Código da Modalidade referente à Contratação                                                                       |
| 8  | modalidadeNome                | String    | Modalidade referente à Contratação                                                                                 |
| 9  | modoDisputald                 | Inteiro   | Código do modo de disputa referente à Contratação                                                                  |
| 10 | modoDisputaNome               | String    | Modo de disputa referente à Contratação                                                                          |
| 11 | situacaoComprald              | Inteiro   | Código da situação da Contratação                                                                                  |
| 12 | situacaoCompraNome            | Inteiro   | Situação da Contratação                                                                                            |
| 13 | objetoCompra                  | Texto (5120)| Descrição do Objeto referente à Contratação                                                                      |
| 14 | informacaoComplementar        | Texto (5120)| Informação Complementar do objeto referente à Contratação                                                          |
| 15 | srp                           | Boleano   | Identifica se a compra trata-se de um SRP (Sistema de registro de preços)                                          |
| 16 | amparoLegal                   |           | Dados do amparo legal                                                                                              |
| 16.1| codigo                        | Inteiro   | Código do Amparo Legal                                                                                             |
| 16.2| nome                          | String    | Nome do Amparo Legal                                                                                               |
| 16.3| descricao                     | String    | Descrição do Amparo legal                                                                                          |
| 17 | valorTotalEstimado            | Decimal   | Valor total estimado da Contratação. Precisão de até 4 dígitos decimais; Ex: 100.0001; Obs: Retornará valor zero (0) se atributo orcamentoSigiloso for true e o item não possuir resultado. |
| 18 | valorTotalHomologado          | Decimal   | Valor total homologado com base nos resultados incluídos. Precisão de até 4 dígitos decimais; Ex: 100.0001;          |
| 19 | dataAberturaProposta          | Data e Hora | Data de abertura do recebimento de propostas (horário de Brasília)                                                 |
| 20 | dataEncerramentoProposta      | Data e Hora | Data de encerramento do recebimento de propostas (horário de Brasília)                                             |
| 21 | dataPublicacaoPncp            | Data      | Data da publicação da Contratação no PNCP                                                                         |
| 22 | datalnclusao                  | Data      | Data da inclusão do registro da Contratação no PNCP                                                                |

---

## 6.5. Serviço Consultar Atas de Registro de Preço por Período de Vigência

Serviço que permite consultar atas de registro de preços publicadas no PNCP por um período informado.

A partir da data inicial e data final informadas, serão recuperadas as atas cujo período de vigência coincida com o período informado. Opcionalmente poderá ser informado CNPJ do Órgão/Entidade, código da Unidade Administrativa do Órgão/Entidade ou número de identificação do Usuário (Portais de Contratações Públicas).

**Detalhes da Requisição**

| Endpoint | Método HTTP | Exemplo de Payload |
|----------|-------------|--------------------|
| /v1/atas | GET         |                    |

**Exemplo Requisição (cURL)**

```bash
curl -X 'GET' \
  '${BASE_URL}/v1/atas?dataInicial=20230701&dataFinal=20230831&pagina=1' \
  -H 'accept: */*'

ou

curl -X 'GET' \
  '${BASE_URL}/v1/atas?dataInicial=20231024&dataFinal=20241023&idUsuario=36&cnpjOrgao=00394429000100&pagina=1' \
  -H 'accept: */*'
```

**Dados de entrada**

*Nota: Dados a serem enviados no cabeçalho da requisição.*

| Campo                         | Tipo    | Obrigatório | Descrição                                                                                                       |
|-------------------------------|---------|-------------|-----------------------------------------------------------------------------------------------------------------|
| dataInicial                   | Data    | Sim         | Data inicial do período a ser consultado no formato AAAAMMDD.                                                 |
| dataFinal                     | Data    | Sim         | Data final do período a ser consultado no formato AAAAMMDD.                                                   |
| idUsuario                     | Inteiro | Não         | Identificado do sistema usuário (Sistema de Contratações Públicas) que publicou a ata.                          |
| cnpj                          | String  | Não         | Cnpj do órgão originário da contratação informado na inclusão (proprietário da contratação)                   |
| codigoUnidadeAdministrativa   | String  | Não         | Código da Unidade Administrativa do Órgão originário da contratação informado na inclusão (proprietário da contratação) |
| pagina                        | Inteiro | Sim         | Número da página que se deseja obter os dados.                                                                  |
| tamanhoPagina                 | Inteiro | Não         | Por padrão cada página contém no máximo 500 registros, no entanto o tamanho de registros em cada página pode ser ajustado (até o limite de 500 registros) com vistas a tornar a entrega de dados mais rápida. |

**Dados de retorno**

| Id | Campo                         | Tipo    | Descrição                                                                |
|----|-------------------------------|---------|--------------------------------------------------------------------------|
| 1  | Atas                          |         | Agrupador da lista de atas                                               |
| 1.1| numeroControlePNCPAta         | String  | Número de Controle PNCP da Ata (id Ata PNCP)                             |
| 1.2| numeroControlePNCPCompra      | String  | Número de Controle PNCP da Contratação (id Contratação PNCP) que a ata está vinculada |
| 1.3| numeroAtaRegistroPreco        | Texto (50)| Número da Ata no sistema de origem                                       |
| 1.4| anoAta                        | Inteiro | Ano da Ata                                                               |
| 1.5| dataAssinatura                | Data    | Data de assinatura da Ata                                                |
| 1.6| vigencialnicio                | Data    | Data de início de vigência da Ata                                        |
| 1.7| vigenciaFim                   | Data    | Data de fim de vigência da Ata                                           |
| 1.8| dataCancelamento              | Data    | Data de cancelamento da Ata                                              |
| 1.9| cancelado                     | Booleano| Indicador de cancelamento da Ata                                         |
| 1.10| dataPublicacaoPncp            | Data    | Data da publicação da Ata no PNCP                                         |
| 1.11| datalnclusao                  | Data    | Data da inclusão do registro da Ata no PNCP                              |
| 1.12| dataAtualizacao               | Data    | Data da última atualização do registro da Ata                           |
| 1.13| objetoContratacao             | String  | Descrição do Objeto referente à Contratação                              |
| 1.14| cnpjOrgao                     | String  | CNPJ do Órgão referente à Contratação                                  |
| 1.15| nomeOrgao                     | String  | Razão Social do Órgão referente à Contratação                          |
| 1.16| codigoUnidadeOrgao            | String  | Código da Unidade Administrativa do Órgão referente à Contratação      |
| 1.17| nomeUnidadeOrgao              | String  | Nome da Unidade Administrativa do Órgão referente à Contratação        |
| 1.18| cnpjOrgaoSubrogado            | String  | CNPJ do Órgão subrogado referente à Contratação                        |
| 1.19| nomeOrgaoSubrogado            | String  | Razão Social do Órgão subrogado referente à Contratação                |
| 1.20| codigoUnidadeOrgaoSubrogado   | String  | Código da Unidade Administrativa subrogada do Órgão subrogado referente à Contratação |
| 1.21| nomeUnidadeOrgaoSubrogado     | String  | Nome da Unidade Administrativa subrogada do Órgão subrogado referente à Contratação |
| 1.22| usuario                       | String  | Nome do sistema usuário (Sistema de Contratações Públicas) que publicou a ata. |

**Códigos de Retorno**

| Código HTTP | Mensagem    | Tipo    |
|-------------|-------------|---------|
| 200         | OK          | Sucesso |
| 204         | No Content  | Sucesso |
| 400         | Bad Request | Erro    |
| 422         | Unprocessable Entity | Erro    |
| 500         | Internal Server Error| Erro    |

---

## 6.6. Serviço Consultar Contratos por Data de Publicação

Serviço que permite consultar contratos e/ou empenhos com força de contrato publicados no PNCP por um período informado. A partir da data inicial e data final informadas serão recuperados os contratos/empenhos publicados no período. Opcionalmente poderá ser informado CNPJ do Órgão/Entidade, código da Unidade Administrativa do Órgão/Entidade ou número de identificação do Usuário (Portais de Contratações Públicas).

**Detalhes da Requisição**

| Endpoint     | Método HTTP | Exemplo de Payload |
|--------------|-------------|--------------------|
| /v1/contratos| GET         |                    |

**Exemplo Requisição (cURL)**

```bash
curl -k -X GET "${BASE_URL}/v1/contratos?dataInicial=20230801&dataFinal=20230831&pagina=1" /
-H "accept: */*"

ou

curl -k -X GET "${BASE_URL}/v1/contratos?dataInicial=20230801&dataFinal=20230831&cnpjOrgao=00394544000185&pagina=1" /
-H "accept: */*"
```

**Dados de entrada**

*Nota: Dados a serem enviados no cabeçalho da requisição.*

| Campo                         | Tipo    | Obrigatório | Descrição                                                                                                       |
|-------------------------------|---------|-------------|-----------------------------------------------------------------------------------------------------------------|
| dataInicial                   | Data    | Sim         | Data inicial do período a ser consultado no formato AAAAMMDD.                                                 |
| dataFinal                     | Data    | Sim         | Data final do período a ser consultado no formato AAAAMMDD.                                                     |
| cnpjOrgao                     | String  | Não         | Cnpj do órgão originário da contratação informado na inclusão (proprietário do contrato)                        |
| codigoUnidadeAdministrativa   | String  | Não         | Código da Unidade Administrativa do Órgão originário da contratação informado na inclusão (proprietário do contrato) |
| usuariold                     | Inteiro | Não         | Identificado do sistema usuário (Sistema de Contratações Públicas) que publicou o contrato.                     |
| pagina                        | Inteiro | Sim         | Número da página a ser requisitada                                                                              |
| tamanhoPagina                 | Inteiro | Não         | Por padrão cada página contém no máximo 500 registros, no entanto o tamanho de registros em cada página pode ser ajustado (até o limite de 500 registros) com vistas a tornar a entrega de dados mais rápida. |

**Dados de retorno**

| Id | Campo                         | Tipo    | Descrição                                                                                                          |
|----|-------------------------------|---------|--------------------------------------------------------------------------------------------------------------------|
| 1  | numeroControlePNCP            | String  | Número de controle PNCP do contrato (id contrato PNCP)                                                             |
| 2  | numeroControlePNCPCompra      | String  | Número de controle PNCP da contratação relacionada (id contratação PNCP)                                         |
| 3  | numeroContratoEmpenho         | Texto (50)| Número do contrato ou empenho com força de contrato                                                              |
| 4  | anoContrato                   | Inteiro | Ano do contrato                                                                                                    |
| 5  | sequentialContrato            | Inteiro | Número sequencial do contrato (gerado pelo PNCP)                                                                   |
| 6  | processo                      | Texto (50)| Número do processo                                                                                                 |
| 7  | tipoContrato                  |         | Dados do tipo de contrato                                                                                          |
| 7.1| Id                            | Inteiro | Código da tabela de domínio Tipo de contrato                                                                       |
| 7.2| Nome                          | String  | Nome do Tipo de Contrato                                                                                           |
| 8  | categoriaProcesso             |         | Dados da categoria do processo                                                                                     |
| 8.1| Id                            | Inteiro | Código da tabela de domínio Categoria                                                                              |
| 8.2| Nome                          | String  | Nome da Categoria do processo                                                                                      |
| 9  | receita                       | Boleano | Receita ou despesa: True - Receita; False - Despesa;                                                               |
| 10 | objetoContrato                | Texto (5120)| Descrição do objeto do contrato                                                                                    |
| 11 | informacaoComplementar        | Texto (5120)| Informações complementares; Se existir;                                                                            |
| 12 | orgaoEntidade                 |         | Dados do Órgão/Entidade do Contrato                                                                                |
| 12.1| cnpj                          | String  | CNPJ do Órgão referente à Contrato                                                                               |
| 12.2| razaoSocial                   | String  | Razão social do Órgão referente à Contrato                                                                       |
| 12.3| poderld                       | String  | Código do poder a que pertence o Órgão. L - Legislativo; E - Executivo; J - Judiciário                           |
| 12.4| esferald                      | String  | Código da esfera a que pertence o Órgão. F - Federal; E - Estadual; M - Municipal; D - Distrital                  |
| 13 | unidadeOrgao                  |         | Dados da Unidade executora do Órgão do Contrato                                                                    |
| 13.1| codigoUnidade                 | String  | Código da Unidade Executora pertencente ao Órgão                                                                   |
| 13.2| nomeUnidade                   | String  | Nome da Unidade Executora pertencente ao Órgão                                                                     |
| 13.3| codigolbge                    | Inteiro | Código IBGE do município                                                                                           |
| 13.4| municipioNome                 | String  | Nome do município                                                                                                  |
| 13.5| ufSigla                       | String  | Sigla da unidade federativa do município                                                                           |
| 13.6| ufNome                        | String  | Nome da unidade federativa do município                                                                            |
| 14 | orgaoSubRogado                |         | Dados do Órgão/Entidade subrogado do Contrato                                                                      |
| 14.1| cnpj                          | String  | CNPJ do Órgão referente à Contrato                                                                               |
| 14.2| razaoSocial                   | String  | Razão social do Órgão referente à Contrato                                                                       |
| 14.3| poderld                       | String  | Código do poder a que pertence o Órgão. L - Legislativo; E - Executivo; J - Judiciário                           |
| 14.4| esferald                      | String  | Código da esfera a que pertence o Órgão. F - Federal; E - Estadual; M - Municipal; D - Distrital                  |
| 15 | unidadeSubRogada              |         | Dados da Unidade Executora do Órgão subrogado                                                                      |
| 15.1| codigoUnidade                 | String  | Código da Unidade Executora pertencente ao Órgão                                                                   |
| 15.2| nomeUnidade                   | String  | Nome da Unidade Executora pertencente ao Órgão                                                                     |
| 15.3| codigolbge                    | Inteiro | Código IBGE do município                                                                                           |
| 15.4| municipioNome                 | String  | Nome do município                                                                                                  |
| 15.5| ufSigla                       | String  | Sigla da unidade federativa do município                                                                           |
| 15.6| ufNome                        | String  | Nome da unidade federativa do município                                                                            |
| 16 | tipoPessoa                    | Texto (2) | PJ - Pessoa jurídica; PF - Pessoa física; PE - Pessoa estrangeira;                                                 |
| 17 | niFornecedor                  | Texto (30)| Número de identificação do fornecedor/arrematante; CNPJ, CPF ou identificador de empresa estrangeira;          |
| 18 | nomeRazaoSocialFornecedor     | Texto (100)| Nome ou razão social do fornecedor/arrematante                                                                     |
| 19 | tipoPessoaSubContratada       | Texto (2) | PJ - Pessoa jurídica; PF - Pessoa física; PE - Pessoa estrangeira; Somente em caso de subcontratação;             |
| 20 | niFornecedorSubContratado     | Texto (30)| Número de identificação do fornecedor subcontratado; CNPJ, CPF ou identificador de empresa estrangeira; Somente em caso de subcontratação; |
| 21 | nomeFornecedorSubContratado   | Texto (100)| Nome ou razão social do fornecedor subcontratado; Somente em caso de subcontratação;                           |
| 22 | valorInicial                  | Decimal | Valor inicial do contrato. Precisão de até 4 dígitos decimais; Ex: 100.0001;                                       |
| 23 | numeroParcelas                | Inteiro | Número de parcelas                                                                                                 |
| 24 | valorParcela                  | Decimal | Valor da parcela. Precisão de até 4 dígitos decimais; Ex: 100.0001;                                                |
| 25 | valorGlobal                   | Decimal | Valor global do contrato. Precisão de até 4 dígitos decimais; Ex: 100.0001;                                        |
| 26 | valorAcumulado                | Decimal | Valor acumulado do contrato. Precisão de até 4 dígitos decimais; Ex: 100.0001;                                       |
| 27 | dataAssinatura                | Data    | Data de assinatura do contrato                                                                                     |
| 28 | dataVigencialnicio            | Data    | Data de início de vigência do contrato                                                                             |
| 29 | dataVigenciaFim               | Data    | Data do término da vigência do contrato                                                                            |
| 30 | numeroRetificacao             | Inteiro | Número de retificações; Número de vezes que este registro está sendo alterado;                                     |
| 31 | usuarioNome                   | String  | Nome do sistema/portal que enviou o contrato                                                                       |
| 32 | dataPublicacaoPncp            | Data/Hora | Data de publicação do contrato no PNCP                                                                             |
| 33 | dataAtualizacao               | Data/Hora | Data da última atualização do contrato no PNCP                                                                     |
| 34 | identificadorCipi             | String  | Identificador do contrato no Cadastro Integrado de Projetos de Investimento                                        |
| 35 | urlCipi                       | String  | Url com informações do contrato no sistema de Cadastro Integrado de Projetos de Investimento                       |

**Códigos de Retorno**

| Código HTTP | Mensagem             | Tipo    |
|-------------|----------------------|---------|
| 200         | OK                   | Sucesso |
| 204         | No Content           | Sucesso |
| 400         | Bad Request          | Erro    |
| 422         | Unprocessable Entity | Erro    |
| 500         | Internal Server Error| Erro    |

---

## 6.6.1 - Observação:

Em adição ao serviço "6.6. Serviço Consultar Contratos por Data de Publicação" mencionado neste manual, é importante destacar que o Portal Nacional de Contratações Públicas (PNCP) oferece uma gama ampla de funcionalidades via API que permitem uma consulta detalhada sobre CONTRATAÇÕES.

Estas funcionalidades estão minuciosamente descritas no Manual de Integração – Portal Nacional de Contratações Públicas - PNCP, disponível no site oficial www.gov.br. Abaixo, apresentamos uma lista com alguns exemplos de serviços disponíveis:

*   6.5.7. Consultar Documento de um Contrato
*   6.5.9. Consultar Contratos de uma Contratação

Recomendamos a leitura detalhada do Manual de Integração do PNCP para uma compreensão abrangente de todas as funcionalidades e possibilidades oferecidas pela API.

---

## 7. Suporte

Em caso de problemas durante o processo de integração do seu sistema com o PNCP, por favor entre em contato com a Central de Atendimento do Ministério da Gestão e da Inovação em Serviços Públicos (https://portaldeservicos.economia.gov.br) ou pelo telefone 0800 978 9001.

---

## 8. Glossário

O seguinte glossário fornece definições e explicações de termos e siglas específicos utilizados ao longo deste documento. O objetivo é esclarecer qualquer ambiguidade e ajudar o leitor a compreender melhor o conteúdo apresentado.

*   **API (Application Programming Interface):** Interface de Programação de Aplicações. É um conjunto de rotinas e padrões estabelecidos por um software para a utilização das suas funcionalidades por programas que não pretendem envolver-se em detalhes da implementação do software, mas apenas usá-lo.
*   **CNBS (Catálogo Nacional de Bens e Serviços):** Catálogo que lista e categoriza bens e serviços. Em muitos contextos, serve como uma referência padronizada para a classificação e descrição de itens. Mais informações em: https://www.gov.br/compras/pt-br/sistemas/conheca-o-compras/catalogo
*   **CNPJ (Cadastro Nacional da Pessoa Jurídica):** É o registro de empresas e outras entidades na Receita Federal do Brasil.
*   **HTTP (Hypertext Transfer Protocol):** Protocolo de Transferência de Hipertexto. É o protocolo fundamental da web, usado para transferir e exibir páginas da web, entre outros.
*   **JSON (JavaScript Object Notation):** Notação de Objeto JavaScript. É um formato de intercâmbio de dados leve e de fácil leitura e escrita para seres humanos.
*   **ME/EPP:** Microempresa e Empresa de Pequeno Porte. São categorias de empresas definidas pela legislação brasileira com base em seu faturamento.
*   **PDM (Padrão Descritivo de Material):** Refere-se a um padrão ou modelo utilizado para descrever materiais de forma consistente e padronizada, facilitando a identificação, catalogação e gestão de materiais em diversos sistemas e contextos.
*   **PCA:** plano de contratações anual definido na lei 14.133/2021
*   **PNCP (Portal Nacional de Contratações Públicas):** sítio oficial estabelecido pela Lei 14133 para divulgação e gestão de contratações públicas no Brasil. Centraliza informações, editais e contratos, promovendo transparência e eficiência, e é gerido por um comitê nacional.
*   **REST (Representational State Transfer):** Transferência de Estado Representacional. É um estilo arquitetural para desenvolvimento de serviços web. É caracterizado por um conjunto de restrições, incluindo um protocolo cliente/servidor sem estado e um conjunto padrão de métodos HTTP.
*   **SWAGGER:** É uma ferramenta de software de código aberto usada para projetar, construir e documentar serviços web REST.
*   **TIC (Tecnologia da Informação e Comunicação):** Refere-se a qualquer tecnologia que ajuda a produzir, manipular, armazenar, comunicar ou disseminar informação.
*   **URL (Uniform Resource Locator):** Localizador Padrão de Recursos. É um endereço de um recurso na web.
*   **USUÁRIO:** Em contextos de sistemas e aplicações, refere-se à pessoa ou entidade que utiliza o software ou sistema em questão.
