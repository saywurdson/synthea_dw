

select 
    icd_10_pcs as code,
    icd_10_pcs_description as code_description,
    prccsr as ccsr_category,
    left(prccsr, 3) as ccsr_parent_category,
    prccsr_description as ccsr_category_description,
    clinical_domain,
   '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."ccsr"."_value_set_prccsr_v2023_1_cleaned_map"