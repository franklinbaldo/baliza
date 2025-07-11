
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select numeroControlePncpCompra
from "baliza_dbt"."psa"."contratos_raw"
where numeroControlePncpCompra is null



  
  
      
    ) dbt_internal_test