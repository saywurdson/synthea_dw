
    
    

select
    (patient_id || '_' || condition) as unique_field,
    count(*) as n_records

from "synthea"."chronic_conditions"."tuva_chronic_conditions_long"
where (patient_id || '_' || condition) is not null
group by (patient_id || '_' || condition)
having count(*) > 1


