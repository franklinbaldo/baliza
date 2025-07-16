Excelente. Com base em todas as nossas discussões, aqui está a versão final e consolidada do plano de implementação. Este documento serve como um guia técnico completo para refatorar o script, adotando a arquitetura orientada por uma tabela de controle de tarefas.

---

### **Plano de Implementação Final: Extração Orientada por Tabela de Controle**

#### 1. Visão Geral e Objetivo

O objetivo é transformar o script de extração de um processo em tempo real para um sistema de ETL (Extração, Transformação e Carga) mais robusto, gerenciado por um estado persistente. Faremos isso introduzindo uma **tabela de controle de tarefas** no DuckDB.

Esta tabela funcionará como um "plano de trabalho" definitivo, listando cada combinação de *endpoint* e *período mensal* como uma tarefa individual. O script passará por fases distintas: planejar o trabalho, descobrir os detalhes de cada tarefa (ex: total de páginas), executar o download e atualizar o progresso.

Esta arquitetura tornará o processo extremamente resiliente a interrupções, totalmente resumível (idempotente) e muito mais fácil de monitorar e depurar.

---

#### 2. Arquitetura Proposta: A Tabela de Controle de Tarefas

Criaremos uma nova tabela, `psa.pncp_extraction_tasks`, que será o cérebro da operação.

**Definição da Tabela (SQL):**
```sql
CREATE TABLE IF NOT EXISTS psa.pncp_extraction_tasks (
    -- Identificadores da Tarefa
    task_id VARCHAR PRIMARY KEY,                  -- Chave primária legível, ex: 'contratos_publicacao_2023-01-01'
    endpoint_name VARCHAR NOT NULL,               -- Nome do endpoint da API
    data_date DATE NOT NULL,                      -- Data de início do período mensal da tarefa

    -- Máquina de Estados e Metadados
    status VARCHAR DEFAULT 'PENDING' NOT NULL,    -- Estado: PENDING, DISCOVERING, FETCHING, PARTIAL, COMPLETE, FAILED
    total_pages INTEGER,                          -- Nº total de páginas (descoberto na Fase 2)
    total_records INTEGER,                        -- Nº total de registros (descoberto na Fase 2)
    
    -- Rastreamento de Progresso
    missing_pages JSON,                           -- Lista JSON de números de página que faltam, ex: '[2, 5, 8]'

    -- Auditoria e Depuração
    last_error TEXT,                              -- Mensagem do último erro para fácil diagnóstico
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),

    -- Restrições para garantir a integridade
    CONSTRAINT unique_task UNIQUE (endpoint_name, data_date)
);
```

**Estados da Tarefa (`status`):**
*   **`PENDING`**: A tarefa foi criada, mas nenhum trabalho foi iniciado.
*   **`DISCOVERING`**: O processo está buscando a página 1 para obter metadados.
*   **`FETCHING`**: A tarefa está ativa, e suas páginas faltantes estão sendo baixadas.
*   **`PARTIAL`**: A tarefa foi parcialmente processada, mas interrompida. Útil para saber quais tarefas retomar primeiro.
*   **`COMPLETE`**: Todas as páginas para esta tarefa foram baixadas e salvas com sucesso.
*   **`FAILED`**: Ocorreu um erro irrecuperável na fase de descoberta ou execução.

---

#### 3. Fluxo de Execução Refatorado em Fases

O método principal `extract_data` orquestrará as seguintes fases de forma sequencial:

**Fase 0: Inicialização**
1.  Conectar ao DuckDB.
2.  Executar o `CREATE TABLE IF NOT EXISTS` para garantir que `psa.pncp_extraction_tasks` exista.

**Fase 1: Planejamento (Gerar Tarefas)**
*   **Objetivo:** Popular a tabela de controle com todas as tarefas necessárias.
*   **Ações:**
    1.  Gerar a lista de períodos mensais (`months_to_process`) a partir da data de início e fim fornecidas.
    2.  Iterar sobre cada `endpoint` e cada `month`.
    3.  Para cada combinação, construir um `task_id` (ex: `f"{endpoint['name']}_{month[0].isoformat()}"`).
    4.  Executar um `INSERT ... ON CONFLICT DO NOTHING` em lote para adicionar apenas as tarefas que ainda não existem na tabela de controle. Isso torna a fase idempotente.

**Fase 2: Descoberta (Discovery)**
*   **Objetivo:** Obter os metadados (`total_pages`, `total_records`) para todas as tarefas pendentes, buscando apenas a página 1 de cada uma.
*   **Ações:**
    1.  Selecionar todas as tarefas com `status = 'PENDING'`.
    2.  Para cada tarefa, em paralelo:
        a.  Atualizar seu status para `DISCOVERING`.
        b.  Fazer uma única requisição para a **página 1** daquele endpoint/período.
        c.  **Em caso de sucesso:**
            i.   Calcular `total_pages` a partir do `totalRegistros` da resposta.
            ii.  Gerar a lista inicial de `missing_pages` (ex: `list(range(2, total_pages + 1))`).
            iii. Atualizar a tarefa na tabela: `status = 'FETCHING'`, preencher `total_pages`, `total_records`, e `missing_pages`.
            iv. Salvar a resposta da página 1 na tabela `psa.pncp_raw_responses` usando o `writer_worker`.
        d.  **Em caso de falha:**
            i.   Atualizar a tarefa na tabela: `status = 'FAILED'`, preencher o campo `last_error`.

**Fase 3: Execução (Fetching)**
*   **Objetivo:** Baixar todas as páginas listadas como "faltantes" no plano de trabalho.
*   **Ações:**
    1.  Construir uma lista global de **todas as páginas a serem baixadas** a partir de todas as tarefas que estão com `status = 'FETCHING'` ou `status = 'PARTIAL'`. A consulta SQL com `unnest` é perfeita para isso:
        ```sql
        SELECT t.task_id, t.endpoint_name, t.data_date, p.page_number
        FROM psa.pncp_extraction_tasks t,
             unnest(json_extract(t.missing_pages, '$')) AS p(page_number)
        WHERE t.status IN ('FETCHING', 'PARTIAL');
        ```
    2.  Criar uma tarefa `asyncio` para cada linha retornada pela consulta acima, passando todos os parâmetros necessários para a função `_fetch_with_backpressure`.
    3.  Executar todas as tarefas de download concorrentemente, respeitando o semáforo.
    4.  Enfileirar todas as respostas (sucesso ou falha) para o `writer_worker`, que as salvará na tabela `psa.pncp_raw_responses`.

**Fase 4: Reconciliação (Atualização de Estado)**
*   **Objetivo:** Atualizar a tabela de controle com base nos dados que foram efetivamente baixados na Fase 3.
*   **Ações:**
    1.  Esta fase é executada após a conclusão da Fase 3 (ou seja, quando a fila de escrita estiver vazia).
    2.  Crie uma função `_reconcile_tasks()`.
    3.  Dentro dela, selecione todas as tarefas com `status IN ('FETCHING', 'PARTIAL')`.
    4.  Para cada tarefa:
        a.  Consulte a tabela `psa.pncp_raw_responses` para obter a lista de páginas que foram baixadas com sucesso (`response_code = 200`) para aquele `endpoint_name` e `data_date`.
        b.  Compare a lista de páginas baixadas com a lista `missing_pages` da tarefa.
        c.  Calcule a nova lista de `missing_pages`.
        d.  **Atualize a tarefa:**
            i.   Se a nova lista `missing_pages` estiver vazia, mude `status = 'COMPLETE'`.
            ii.  Se a lista diminuiu mas não está vazia, mude `status = 'PARTIAL'` (ou mantenha `FETCHING`).
            iii. Atualize a coluna `missing_pages` com a nova lista.

---

#### 4. Integração com a Interface do Usuário (`rich.Progress`)

A nova arquitetura permite um feedback visual muito mais rico e preciso.

1.  **Progresso Geral:** Uma barra de progresso principal pode mostrar o avanço total do plano de trabalho.
    *   `total = SELECT COUNT(*) FROM psa.pncp_extraction_tasks;`
    *   `completed = SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'COMPLETE';`

2.  **Progresso por Fase:** Você pode ter barras de progresso para cada fase:
    *   **Descoberta:** Uma barra mostrando o processamento das tarefas `PENDING`.
    *   **Execução:** Uma barra mostrando o número de páginas baixadas vs. o total de páginas faltantes no início da fase.

3.  **Progresso Detalhado (Opcional):** É possível até mesmo mostrar o progresso individual de cada tarefa ativa (`status = 'FETCHING'`), calculando o avanço com base no tamanho da lista `missing_pages`.

---

#### 5. Resumo das Vantagens da Nova Arquitetura

*   **Resiliência Total:** O script pode ser interrompido a qualquer momento e retomará exatamente de onde parou.
*   **Idempotência:** Execuções repetidas não causam duplicação de dados ou trabalho desnecessário.
*   **Transparência e Depuração:** A tabela de controle fornece uma visão clara do estado de cada parte do processo, facilitando a identificação de falhas.
*   **Escalabilidade:** O modelo de "unidades de trabalho" (páginas) pode ser facilmente paralelizado ou até distribuído entre múltiplos processos/máquinas.
*   **Manutenibilidade:** A separação clara de fases torna o código mais limpo, organizado e fácil de entender e modificar.