
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select endpoint_name
from "baliza"."psa"."pncp_raw_responses"
where endpoint_name is null



  
  
      
    ) dbt_internal_test