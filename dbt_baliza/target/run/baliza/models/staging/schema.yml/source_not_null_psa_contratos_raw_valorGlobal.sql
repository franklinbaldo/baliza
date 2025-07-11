
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valorGlobal
from "baliza_dbt"."psa"."contratos_raw"
where valorGlobal is null



  
  
      
    ) dbt_internal_test