
    
    

with all_values as (

    select
        response_code as value_field,
        count(*) as n_records

    from "baliza"."psa"."pncp_raw_responses"
    group by response_code

)

select *
from all_values
where value_field not in (
    '200','204','400','401','422','429','500','502','503','504'
)


