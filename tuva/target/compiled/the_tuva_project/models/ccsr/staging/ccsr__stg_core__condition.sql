

select *, '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."condition"
where normalized_code_type = 'icd-10-cm'