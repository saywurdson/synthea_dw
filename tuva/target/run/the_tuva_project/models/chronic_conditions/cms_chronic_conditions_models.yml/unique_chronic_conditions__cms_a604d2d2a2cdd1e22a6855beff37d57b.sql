select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    (patient_id || claim_id || start_date || condition) as unique_field,
    count(*) as n_records

from "synthea"."chronic_conditions"."cms_chronic_conditions_long"
where (patient_id || claim_id || start_date || condition) is not null
group by (patient_id || claim_id || start_date || condition)
having count(*) > 1



      
    ) dbt_internal_test