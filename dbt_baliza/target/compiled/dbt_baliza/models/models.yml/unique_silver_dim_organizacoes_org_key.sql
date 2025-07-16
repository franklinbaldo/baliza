



select
    org_key as unique_field,
    count(*) as n_records

from "baliza"."main_silver"."silver_dim_organizacoes"
where org_key is not null
group by org_key
having count(*) > 1
