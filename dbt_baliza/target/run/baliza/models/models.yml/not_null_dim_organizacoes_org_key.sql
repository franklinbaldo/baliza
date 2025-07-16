
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (








select org_key
from "baliza"."main_dimensions"."dim_organizacoes"
where org_key is null






    ) dbt_internal_test