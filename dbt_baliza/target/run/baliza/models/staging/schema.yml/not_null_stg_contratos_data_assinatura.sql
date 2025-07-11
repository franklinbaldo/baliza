
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select data_assinatura
from "baliza_dbt"."main_staging"."stg_contratos"
where data_assinatura is null



  
  
      
    ) dbt_internal_test