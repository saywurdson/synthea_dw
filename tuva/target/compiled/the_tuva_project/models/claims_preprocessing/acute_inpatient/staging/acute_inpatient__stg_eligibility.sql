

select
    patient_id
    , birth_date
    , gender
    , race
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_eligibility"