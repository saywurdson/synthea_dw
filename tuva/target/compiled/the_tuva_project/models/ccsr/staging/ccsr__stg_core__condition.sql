

select *, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."condition"
where normalized_code_type = 'icd-10-cm'