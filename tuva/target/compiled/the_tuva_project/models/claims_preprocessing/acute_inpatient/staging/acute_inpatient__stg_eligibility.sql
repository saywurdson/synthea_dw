

select
    patient_id
    , birth_date
    , gender
    , race
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_eligibility"