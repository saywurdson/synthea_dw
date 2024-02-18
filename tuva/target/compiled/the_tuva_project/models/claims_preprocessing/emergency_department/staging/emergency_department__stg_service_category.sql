


select
    claim_id
    , claim_type
    , claim_line_number
    , service_category_2
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."service_category_grouper"