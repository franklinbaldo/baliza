
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select extracted_at
from "baliza"."psa"."pncp_raw_responses"
where extracted_at is null



  
  
      
    ) dbt_internal_test