
    
    

select
    patient_id as unique_field,
    count(*) as n_records

from "synthea"."chronic_conditions"."cms_chronic_conditions_wide"
where patient_id is not null
group by patient_id
having count(*) > 1


