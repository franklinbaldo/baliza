
    
    

select
    id as unique_field,
    count(*) as n_records

from "baliza"."psa"."pncp_raw_responses"
where id is not null
group by id
having count(*) > 1


