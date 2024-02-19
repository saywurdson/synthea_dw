
    
    

select
    (patient_id || '_' || year_month || '_' || plan) as unique_field,
    count(*) as n_records

from "synthea"."financial_pmpm"."pmpm_prep"
where (patient_id || '_' || year_month || '_' || plan) is not null
group by (patient_id || '_' || year_month || '_' || plan)
having count(*) > 1


