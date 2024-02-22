

select
    patient_id
    , birth_date
    , gender
    , race
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_eligibility"