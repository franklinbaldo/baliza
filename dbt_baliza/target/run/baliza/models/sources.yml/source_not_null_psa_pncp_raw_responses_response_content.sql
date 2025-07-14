
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select response_content
from "baliza"."psa"."pncp_raw_responses"
where response_content is null



  
  
      
    ) dbt_internal_test