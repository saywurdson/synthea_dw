

select
    cast(procedure_id as TEXT ) as procedure_id 
    , cast(patient_id as TEXT ) as patient_id
    , cast(encounter_id as TEXT ) as encounter_id
    , cast(claim_id as TEXT ) as claim_id
    , try_cast( procedure_date as date ) as procedure_date
    , cast(source_code_type as TEXT ) as source_code_type
    , cast(source_code as TEXT ) as source_code
    , cast(source_description as TEXT ) as source_description
    , cast(normalized_code_type as TEXT ) as normalized_code_type
    , cast(normalized_code as TEXT ) as normalized_code
    , cast(normalized_description as TEXT ) as normalized_description
    , cast(modifier_1 as TEXT ) as modifier_1
    , cast(modifier_2 as TEXT ) as modifier_2
    , cast(modifier_3 as TEXT ) as modifier_3
    , cast(modifier_4 as TEXT ) as modifier_4
    , cast(modifier_5 as TEXT ) as modifier_5
    , cast(practitioner_id as TEXT ) as practitioner_id
    , cast(data_source as TEXT ) as data_source
    , cast('2024-02-22 00:26:23.471542+00:00' as timestamp ) as tuva_last_run
from "synthea"."tuva_input"."procedure"