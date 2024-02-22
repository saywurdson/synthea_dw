

select *, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."condition"
where normalized_code_type = 'icd-10-cm'