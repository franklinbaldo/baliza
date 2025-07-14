
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select response_code
from "baliza"."psa"."pncp_raw_responses"
where response_code is null



  
  
      
    ) dbt_internal_test