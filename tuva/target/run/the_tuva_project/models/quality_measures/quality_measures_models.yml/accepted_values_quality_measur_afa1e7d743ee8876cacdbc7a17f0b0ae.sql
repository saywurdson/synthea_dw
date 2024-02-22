select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with  __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."patient"
), all_values as (

    select
        sex as value_field,
        count(*) as n_records

    from __dbt__cte__quality_measures__stg_core__patient
    group by sex

)

select *
from all_values
where value_field not in (
    'female','male','unknown'
)



      
    ) dbt_internal_test