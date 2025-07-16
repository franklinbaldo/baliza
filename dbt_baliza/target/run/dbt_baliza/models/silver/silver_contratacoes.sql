

            delete from "baliza"."main_silver"."silver_contratacoes"
            where (
                numero_controle_pncp) in (
                select (numero_controle_pncp)
                from "silver_contratacoes__dbt_tmp20250716220348755967"
            );




    insert into "baliza"."main_silver"."silver_contratacoes" ("response_id", "extracted_at", "endpoint_name", "endpoint_url", "data_date", "run_id", "total_records", "total_pages", "current_page", "record_index", "numero_controle_pncp", "numero_compra", "ano_compra", "sequencial_compra", "data_publicacao_pncp", "data_abertura_proposta", "data_encerramento_proposta", "data_inclusao", "data_atualizacao", "data_atualizacao_global", "valor_total_estimado", "valor_total_homologado", "objeto_compra", "informacao_complementar", "processo", "link_sistema_origem", "link_processo_eletronico", "justificativa_presencial", "modalidade_id", "modalidade_nome", "modo_disputa_id", "modo_disputa_nome", "tipo_instrumento_convocatorio_codigo", "tipo_instrumento_convocatorio_nome", "situacao_compra_id", "situacao_compra_nome", "srp", "existe_resultado", "orgao_entidade_json", "unidade_orgao_json", "orgao_subrogado_json", "unidade_subrogada_json", "amparo_legal_json", "fontes_orcamentarias_json", "usuario_nome", "procurement_json")
    (
        select "response_id", "extracted_at", "endpoint_name", "endpoint_url", "data_date", "run_id", "total_records", "total_pages", "current_page", "record_index", "numero_controle_pncp", "numero_compra", "ano_compra", "sequencial_compra", "data_publicacao_pncp", "data_abertura_proposta", "data_encerramento_proposta", "data_inclusao", "data_atualizacao", "data_atualizacao_global", "valor_total_estimado", "valor_total_homologado", "objeto_compra", "informacao_complementar", "processo", "link_sistema_origem", "link_processo_eletronico", "justificativa_presencial", "modalidade_id", "modalidade_nome", "modo_disputa_id", "modo_disputa_nome", "tipo_instrumento_convocatorio_codigo", "tipo_instrumento_convocatorio_nome", "situacao_compra_id", "situacao_compra_nome", "srp", "existe_resultado", "orgao_entidade_json", "unidade_orgao_json", "orgao_subrogado_json", "unidade_subrogada_json", "amparo_legal_json", "fontes_orcamentarias_json", "usuario_nome", "procurement_json"
        from "silver_contratacoes__dbt_tmp20250716220348755967"
    )
