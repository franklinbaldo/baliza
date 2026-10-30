# Recuperação da publicação do PNCP

A falha #708 ocorreu depois da coleta, no PUT de
`raw-publicacoes-2026-10.zip`: `total_tasks_queued exceeds global_limit`.
Isso indica capacidade global do Internet Archive, não falha no PNCP.
O run 41046587002 não salvou o cache após a falha e não reteve seu ZIP
temporário; não há como prometer recuperar aquele ZIP específico.

## Política de envio

`ia_retry.upload_with_retry` distingue capacidade global, erros temporários
(408/429/500/502/503/504, timeout e conexão) e erros definitivos (incluindo
400/401/403 sem a mensagem específica da fila). Erros definitivos não esperam.
São no máximo seis tentativas e 900 segundos acumulados de espera por envio.
A espera exponencial começa em 60 segundos para capacidade, 30 para outros
transientes, e chega a 300 segundos. `Retry-After`, numérico ou HTTP-date,
é respeitado como mínimo; se ultrapassar o orçamento, a execução falha para
retomada posterior. Os retries internos de upload do SDK ficam desativados.
O timeout HTTP do SDK permanece em 120 segundos por operação; o limite de
900 segundos é de espera, não uma promessa de duração total do pipeline.

## Checkpoint

O ZIP é preparado atomicamente em `data/pending/<recurso>/<partição>/`.
Uma falha de envio preserva seus bytes. A próxima execução publica esse
checkpoint antes de consultar novamente o PNCP. PUTs usam os mesmos nomes
remotos; não criam itens ou arquivos com sufixos por tentativa. Uma confirmação
de envio gera `uploaded.sha256`; falhas posteriores no manifesto retomam a
atualização do manifesto sem repetir o PUT do ZIP. Um timeout com aceitação
incerta pode repetir o PUT dos mesmos bytes.

O checkpoint só é removido após atualização bem-sucedida do manifesto. O
workflow usa restore/save separados, salva mesmo após falha e tem chaves novas
por run/tentativa. Execuções schedule/dispatch do workflow são serializadas.
O cache do GitHub é uma proteção operacional sujeita a quota e expiração;
um encerramento abrupto do runner antes do save ainda pode perder o estado.

## Completude

Os três contratos da PR #696 foram incorporados: falhas de probe/página
abortam antes de publicar, e `_coverage.json` registra as 14 modalidades.
Além das contagens, a evidência contém as páginas esperadas e SHA-256 de cada
arquivo. Upload e retomada verificam a evidência contra os bytes arquivados.
Um HTTP 204 confirmado é vazio; timeout ou corpo vazio não é prova de vazio.

O build verifica o checksum do ZIP contra o manifesto, a cobertura e o
fechamento do mês antes de iniciar a ingestão. ZIPs legados sem evidência
não recebem certificação retroativa. Publicar uma nova versão raw invalida o
timestamp/schema de build anterior para exigir um novo Parquet. Uma captura
com o mês aberto pode ser retomada, mas não vira mensal fechado; seu manifesto
mantém a partição pendente para uma coleta final após a virada do mês.

Esta correção não audita nem certifica o histórico existente e não inicia
backfill automaticamente para todo mês legado já presente no manifesto.

## Verificação operacional

O workflow diário está agendado para 05:00 UTC. Após o próximo ciclo,
verificar o resultado do run e o ZIP esperado no item `baliza-pncp-raw`,
incluindo tamanho, SHA-256 e `_coverage.json`. Conferir no manifesto a linha
`table_name=publicacoes`, `data_particao`, `raw_coverage_complete` e
`raw_partition_closed`. Parquet mensal canônico é esperado apenas para meses
fechados com cobertura certificada; conferir seu checksum, schema e timestamp.
Não fechar #708 somente porque os testes unitários passaram.

Para reproduzir sem coleta externa:

```sh
uv run pytest tests/unit/test_ia_publication_recovery.py tests/unit/test_publicacoes_completeness_contract.py -q
uv run ruff check src/ tests/ scripts/
uv run mypy src/baliza
```
