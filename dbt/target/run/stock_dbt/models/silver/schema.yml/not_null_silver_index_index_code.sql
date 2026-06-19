
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select index_code
from "stock_db"."public_silver"."silver_index"
where index_code is null



  
  
      
    ) dbt_internal_test