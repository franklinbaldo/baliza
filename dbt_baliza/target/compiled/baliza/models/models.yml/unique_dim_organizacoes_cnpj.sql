



select
    cnpj as unique_field,
    count(*) as n_records

from "baliza"."main_dimensions"."dim_organizacoes"
where cnpj is not null
group by cnpj
having count(*) > 1
