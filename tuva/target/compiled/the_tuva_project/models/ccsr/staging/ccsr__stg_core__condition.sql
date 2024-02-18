

select *, '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."condition"
where normalized_code_type = 'icd-10-cm'