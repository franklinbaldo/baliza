
    
    

with all_values as (

    select
        endpoint_name as value_field,
        count(*) as n_records

    from "baliza"."psa"."pncp_raw_responses"
    group by endpoint_name

)

select *
from all_values
where value_field not in (
    'contratos_publicacao','contratos_atualizacao','contratacoes_publicacao','contratacoes_atualizacao','contratacoes_proposta','atas_periodo','atas_atualizacao','instrumentos_cobranca','pca_atualizacao'
)


