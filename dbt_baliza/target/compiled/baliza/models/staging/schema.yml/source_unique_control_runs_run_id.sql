
    
    

select
    run_id as unique_field,
    count(*) as n_records

from "baliza_dbt"."control"."runs"
where run_id is not null
group by run_id
having count(*) > 1


