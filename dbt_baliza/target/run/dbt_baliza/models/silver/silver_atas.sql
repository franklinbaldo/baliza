

            delete from "baliza"."main_silver"."silver_atas"
            where (
                numero_controle_pncp) in (
                select (numero_controle_pncp)
                from "silver_atas__dbt_tmp20250716220348715993"
            );




    insert into "baliza"."main_silver"."silver_atas" ("response_id", "extracted_at", "endpoint_name", "endpoint_url", "data_date", "run_id", "total_records", "total_pages", "current_page", "record_index", "numero_controle_pncp", "numero_ata", "ano_ata", "data_assinatura", "data_vigencia_inicio", "data_vigencia_fim", "data_publicacao_pncp", "data_atualizacao", "ni_fornecedor", "nome_razao_social_fornecedor", "objeto_ata", "informacao_complementar", "numero_retificacao", "orgao_entidade_json", "unidade_orgao_json", "ata_json")
    (
        select "response_id", "extracted_at", "endpoint_name", "endpoint_url", "data_date", "run_id", "total_records", "total_pages", "current_page", "record_index", "numero_controle_pncp", "numero_ata", "ano_ata", "data_assinatura", "data_vigencia_inicio", "data_vigencia_fim", "data_publicacao_pncp", "data_atualizacao", "ni_fornecedor", "nome_razao_social_fornecedor", "objeto_ata", "informacao_complementar", "numero_retificacao", "orgao_entidade_json", "unidade_orgao_json", "ata_json"
        from "silver_atas__dbt_tmp20250716220348715993"
    )
