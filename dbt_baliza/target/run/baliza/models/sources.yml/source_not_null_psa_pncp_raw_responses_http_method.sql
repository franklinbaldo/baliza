
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select http_method
from "baliza"."psa"."pncp_raw_responses"
where http_method is null



  
  
      
    ) dbt_internal_test