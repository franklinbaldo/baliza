
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valor_global_brl
from "baliza_dbt"."main_staging"."stg_contratos"
where valor_global_brl is null



  
  
      
    ) dbt_internal_test