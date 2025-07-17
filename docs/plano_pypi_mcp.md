1.  **Análise do Repositório e Definição dos Critérios de Sucesso.**
    *   Mapear a estrutura atual do código-fonte, identificando os principais componentes, módulos e dependências.
    *   Revisar os planos existentes (`sharding.md`, `task-table-design.md`) para entender completamente a arquitetura de dados e o pipeline de ETL.
    *   Executar os testes existentes para garantir que o ambiente de desenvolvimento está configurado corretamente.
    *   Definir e documentar os critérios de sucesso para a funcionalidade `baliza mcp`, incluindo instalação, conexão com LLM, consulta ao dataset, qualidade da resposta, desempenho e documentação.

2.  **Limpeza da Documentação e Criação do Novo Plano.**
    *   Deletar o plano obsoleto `docs/modularizacao/plano_modularizacao.md`.
    *   Criar o arquivo `docs/plano_pypi_mcp.md` e salvar este plano detalhado nele.

3.  **Estruturação do Pacote PyPI.**
    *   Revisar e adaptar a estrutura do projeto para seguir as convenções de um pacote Python distribuível.
    *   Criar e configurar o arquivo `pyproject.toml` para gerenciar o projeto, suas dependências e metadados.
    *   Definir o ponto de entrada `baliza` no `pyproject.toml` para que o comando fique disponível após a instalação.

4.  **Implementação do Comando `baliza mcp`.**
    *   Criar um novo módulo `src/baliza/mcp.py` para encapsular a lógica da nova funcionalidade.
    *   Adicionar o comando `mcp` no arquivo `src/baliza/cli.py`, integrando-o com a estrutura de linha de comando existente.
    *   Implementar a lógica para conectar-se a serviços de LLM (ex: Anthropic, OpenAI) e gerenciar chaves de API.
    *   Desenvolver a funcionalidade para acessar e consultar os arquivos Parquet do dataset no Internet Archive usando DuckDB.
    *   Implementar o fluxo de consulta, onde a pergunta do usuário é enriquecida com contexto do dataset antes de ser enviada ao LLM.

5.  **Desenvolvimento de Testes.**
    *   Criar um novo arquivo de teste `tests/test_mcp.py`.
    *   Escrever testes unitários para as funções no módulo `mcp.py`, utilizando mocks para as chamadas de API externas.
    *   Escrever testes de integração para verificar o fluxo completo do comando `baliza mcp`.

6.  **Atualização da Documentação do Usuário.**
    *   Atualizar o arquivo `README.md` com instruções claras sobre como instalar o pacote (`pip install baliza`), configurar a chave de API do LLM e utilizar o novo comando `baliza mcp`.
    *   Adicionar exemplos práticos de uso.

7.  **Construção e Publicação do Pacote.**
    *   Construir o pacote nos formatos `sdist` e `wheel` usando a ferramenta `build`.
    *   Publicar o pacote no TestPyPI para uma verificação final.
    *   Após a validação, publicar o pacote no índice oficial do PyPI.
