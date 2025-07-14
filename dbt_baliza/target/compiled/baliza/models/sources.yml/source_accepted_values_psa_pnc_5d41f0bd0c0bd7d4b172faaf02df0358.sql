
    
    

with all_values as (

    select
        http_method as value_field,
        count(*) as n_records

    from "baliza"."psa"."pncp_raw_responses"
    group by http_method

)

select *
from all_values
where value_field not in (
    'GET','POST','PUT','DELETE'
)


