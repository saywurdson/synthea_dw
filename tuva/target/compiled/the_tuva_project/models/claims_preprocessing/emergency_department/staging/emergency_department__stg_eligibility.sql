

select
    patient_id
    , birth_date
    , gender
    , race
    , '2024-02-21 14:30:54.308435+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_eligibility"