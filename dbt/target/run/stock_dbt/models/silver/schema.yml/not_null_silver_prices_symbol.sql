
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from "stock_db"."public_silver"."silver_prices"
where symbol is null



  
  
      
    ) dbt_internal_test