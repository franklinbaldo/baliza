# Investigação da API do PNCP

Este diretório contém os artefatos e evidências da investigação realizada para entender e validar os endpoints da API do Portal Nacional de Contratações Públicas (PNCP).

O trabalho aqui documentado foi crucial para descobrir `modalidades` de contratação não documentadas e garantir que o extrator de dados do BALIZA pudesse buscar todas as informações disponíveis.

## Arquivos

- **`discover_modalidades.py`**: Script utilizado para iterar e testar sistematicamente diferentes IDs de modalidades, a fim de descobrir quais eram válidas.
- **`modalidades_discovered.json`**: Resultado do script acima, listando todas as modalidades encontradas, incluindo as que não estavam na documentação oficial.
- **`test_endpoints_*.py`**: Versões dos scripts de teste usados para validar o comportamento de cada endpoint da API em diferentes estágios da investigação.
- **`endpoint_test_results_*.json`**: Arquivos JSON com os resultados brutos dos testes executados pelos scripts acima.
- **`ENDPOINT_TESTING_REPORT.md`**: Relatório detalhado que consolida as descobertas, análises e conclusões da investigação dos endpoints.

Esses arquivos são mantidos como um registro histórico do processo de engenharia reversa e validação que permitiu a cobertura de dados completa do PNCP pelo projeto BALIZA.
