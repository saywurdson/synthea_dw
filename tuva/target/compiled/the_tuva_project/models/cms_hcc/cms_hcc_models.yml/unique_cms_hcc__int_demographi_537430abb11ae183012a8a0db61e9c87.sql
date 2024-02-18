
    
    

select
    (patient_id||'-'||model_version) as unique_field,
    count(*) as n_records

from "synthea"."cms_hcc"."_int_demographic_factors"
where (patient_id||'-'||model_version) is not null
group by (patient_id||'-'||model_version)
having count(*) > 1


