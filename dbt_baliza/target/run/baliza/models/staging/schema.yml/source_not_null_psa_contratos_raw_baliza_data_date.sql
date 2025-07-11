
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select baliza_data_date
from "baliza_dbt"."psa"."contratos_raw"
where baliza_data_date is null



  
  
      
    ) dbt_internal_test