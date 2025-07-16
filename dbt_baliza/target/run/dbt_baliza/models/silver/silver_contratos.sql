

            delete from "baliza"."main_silver"."silver_contratos"
            where (
                numero_controle_pncp) in (
                select (numero_controle_pncp)
                from "silver_contratos__dbt_tmp20250716220348721831"
            );




    insert into "baliza"."main_silver"."silver_contratos" ("response_id", "extracted_at", "endpoint_name", "endpoint_url", "data_date", "run_id", "total_records", "total_pages", "current_page", "record_index", "numero_controle_pncp", "numero_controle_pncp_compra", "numero_contrato_empenho", "ano_contrato", "sequencial_contrato", "data_assinatura", "data_vigencia_inicio", "data_vigencia_fim", "data_publicacao_pncp", "data_atualizacao", "data_atualizacao_global", "valor_inicial", "valor_global", "valor_parcela", "valor_acumulado", "ni_fornecedor", "tipo_pessoa", "nome_razao_social_fornecedor", "ni_fornecedor_subcontratado", "nome_fornecedor_subcontratado", "tipo_pessoa_subcontratada", "objeto_contrato", "informacao_complementar", "processo", "numero_parcelas", "numero_retificacao", "receita", "orgao_entidade_json", "unidade_orgao_json", "orgao_subrogado_json", "unidade_subrogada_json", "tipo_contrato_json", "categoria_processo_json", "codigo_pais_fornecedor", "identificador_cipi", "url_cipi", "usuario_nome", "contract_json")
    (
        select "response_id", "extracted_at", "endpoint_name", "endpoint_url", "data_date", "run_id", "total_records", "total_pages", "current_page", "record_index", "numero_controle_pncp", "numero_controle_pncp_compra", "numero_contrato_empenho", "ano_contrato", "sequencial_contrato", "data_assinatura", "data_vigencia_inicio", "data_vigencia_fim", "data_publicacao_pncp", "data_atualizacao", "data_atualizacao_global", "valor_inicial", "valor_global", "valor_parcela", "valor_acumulado", "ni_fornecedor", "tipo_pessoa", "nome_razao_social_fornecedor", "ni_fornecedor_subcontratado", "nome_fornecedor_subcontratado", "tipo_pessoa_subcontratada", "objeto_contrato", "informacao_complementar", "processo", "numero_parcelas", "numero_retificacao", "receita", "orgao_entidade_json", "unidade_orgao_json", "orgao_subrogado_json", "unidade_subrogada_json", "tipo_contrato_json", "categoria_processo_json", "codigo_pais_fornecedor", "identificador_cipi", "url_cipi", "usuario_nome", "contract_json"
        from "silver_contratos__dbt_tmp20250716220348721831"
    )
