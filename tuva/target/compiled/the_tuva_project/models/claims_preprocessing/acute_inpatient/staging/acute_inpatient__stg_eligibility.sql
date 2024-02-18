

select
    patient_id
    , birth_date
    , gender
    , race
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_eligibility"