

            delete from "baliza"."main_bronze"."bronze_contratacoes"
            where (
                id) in (
                select (id)
                from "bronze_contratacoes__dbt_tmp20250716220348435000"
            );




    insert into "baliza"."main_bronze"."bronze_contratacoes" ("id", "extracted_at", "endpoint_name", "endpoint_url", "data_date", "run_id", "total_records", "total_pages", "current_page", "response_json")
    (
        select "id", "extracted_at", "endpoint_name", "endpoint_url", "data_date", "run_id", "total_records", "total_pages", "current_page", "response_json"
        from "bronze_contratacoes__dbt_tmp20250716220348435000"
    )
