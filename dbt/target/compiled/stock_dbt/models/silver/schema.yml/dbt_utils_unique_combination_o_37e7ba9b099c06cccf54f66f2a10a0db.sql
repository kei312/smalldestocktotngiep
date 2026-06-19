





with validation_errors as (

    select
        index_code, trade_date
    from "stock_db"."public_silver"."silver_index"
    group by index_code, trade_date
    having count(*) > 1

)

select *
from validation_errors


