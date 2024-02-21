

select
    cast(practitioner_id as TEXT ) as practitioner_id
    , cast(npi as TEXT ) as npi
    , cast(first_name as TEXT ) as first_name
    , cast(last_name as TEXT ) as last_name
    , cast(practice_affiliation as TEXT ) as practice_affiliation
    , cast(specialty as TEXT ) as specialty
    , cast(sub_specialty as TEXT ) as sub_specialty
    , cast(data_source as TEXT ) as data_source
    , cast('2024-02-21 20:38:46.882654+00:00' as timestamp ) as tuva_last_run
from "synthea"."tuva_input"."practitioner"