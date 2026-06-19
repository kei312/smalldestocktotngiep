





with validation_errors as (

    select
        symbol, trade_date
    from "stock_db"."public_silver"."silver_prices"
    group by symbol, trade_date
    having count(*) > 1

)

select *
from validation_errors


