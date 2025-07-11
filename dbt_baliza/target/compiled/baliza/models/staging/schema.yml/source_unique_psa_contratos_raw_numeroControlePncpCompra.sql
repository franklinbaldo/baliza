
    
    

select
    numeroControlePncpCompra as unique_field,
    count(*) as n_records

from "baliza_dbt"."psa"."contratos_raw"
where numeroControlePncpCompra is not null
group by numeroControlePncpCompra
having count(*) > 1


