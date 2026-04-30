# Characterization Tests

Esta pasta contém testes de caracterização (no estilo de Michael Feathers, "Working Effectively with Legacy Code") focados no pipeline atual de extração e consolidação (`contratos`).

O objetivo destes testes é documentar e travar o comportamento atual do sistema, especificamente os pontos de acoplamento hardcoded, **antes** de qualquer refatoração estrutural (como as propostas no plano `multi-table-pipeline`).

Esses testes não devem ser alterados de imediato durante o refator, e servirão como rede de segurança: se um refator acidentalmente quebrar o comportamento existente, esses testes falharão.

Eles cobrem:
- O parse de partição (`%Y-%m`) no `builder.py`.
- O dedup usando uma única PK no `engine.py`.
- O manifesto contendo campos específicos hardcoded no `ia_uploader.py`.
- As tabelas sendo carregadas pelo frontend no `schema.ts`.
- O mock do DB em `build-data.mjs`.

Não adicione testes de novas funcionalidades aqui. Use os testes unitários ou de integração normais para isso.
