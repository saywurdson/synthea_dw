select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        condition as value_field,
        count(*) as n_records

    from "synthea"."chronic_conditions"."_int_cms_chronic_condition_hiv_aids"
    group by condition

)

select *
from all_values
where value_field not in (
    'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)'
)



      
    ) dbt_internal_test