
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select baliza_run_id
from "baliza_dbt"."psa"."contratos_raw"
where baliza_run_id is null



  
  
      
    ) dbt_internal_test