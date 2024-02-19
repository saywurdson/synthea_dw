
select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."patient"