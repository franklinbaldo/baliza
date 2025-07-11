
    
    

select
    contrato_id as unique_field,
    count(*) as n_records

from "baliza_dbt"."main_staging"."stg_contratos"
where contrato_id is not null
group by contrato_id
having count(*) > 1


