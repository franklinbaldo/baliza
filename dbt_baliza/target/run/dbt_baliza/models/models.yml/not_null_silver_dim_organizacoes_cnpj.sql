
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (








select cnpj
from "baliza"."main_silver"."silver_dim_organizacoes"
where cnpj is null






    ) dbt_internal_test