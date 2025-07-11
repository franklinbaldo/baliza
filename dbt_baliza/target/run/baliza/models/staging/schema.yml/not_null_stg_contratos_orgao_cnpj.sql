
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select orgao_cnpj
from "baliza_dbt"."main_staging"."stg_contratos"
where orgao_cnpj is null



  
  
      
    ) dbt_internal_test