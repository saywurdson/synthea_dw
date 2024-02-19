

select 
    encounter_id,
    claim_id,
    patient_id,
    ccsr_category,
    ccsr_category_description,
    ccsr_parent_category,
    parent_category_description,
    body_system,
    '2023.1' as dxccsr_version,
    '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."ccsr"."long_condition_category"
where 
    is_ip_default_category = true
    and condition_rank = 1