
    
    

select
    patient_id as unique_field,
    count(*) as n_records

from "synthea"."cms_hcc"."_int_members"
where patient_id is not null
group by patient_id
having count(*) > 1


