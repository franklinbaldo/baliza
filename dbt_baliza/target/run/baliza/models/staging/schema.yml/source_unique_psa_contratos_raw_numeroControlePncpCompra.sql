
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    numeroControlePncpCompra as unique_field,
    count(*) as n_records

from "baliza_dbt"."psa"."contratos_raw"
where numeroControlePncpCompra is not null
group by numeroControlePncpCompra
having count(*) > 1



  
  
      
    ) dbt_internal_test