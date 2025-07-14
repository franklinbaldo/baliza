
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select endpoint_url
from "baliza"."psa"."pncp_raw_responses"
where endpoint_url is null



  
  
      
    ) dbt_internal_test