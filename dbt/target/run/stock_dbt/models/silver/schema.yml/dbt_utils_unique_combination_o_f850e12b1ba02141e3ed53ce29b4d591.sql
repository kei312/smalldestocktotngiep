
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        symbol, trade_date
    from "stock_db"."public_silver"."silver_prices"
    group by symbol, trade_date
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test