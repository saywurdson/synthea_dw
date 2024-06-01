

with  __dbt__cte__cms_chronic_conditions__stg_core__condition as (


select
      claim_id
    , patient_id
    , recorded_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."condition"
),  __dbt__cte__cms_chronic_conditions__stg_medical_claim as (


select
      claim_id
    , patient_id
    , claim_start_date
    , ms_drg_code
    , data_source
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."tuva_input"."medical_claim"
),  __dbt__cte__cms_chronic_conditions__stg_core__procedure as (


select
      claim_id
    , patient_id
    , procedure_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."procedure"
), chronic_conditions as (

    select * from "synthea"."chronic_conditions"."_value_set_cms_chronic_conditions_hierarchy"

)

, patient_conditions as (

    select
          patient_id
        , claim_id
        , recorded_date as start_date
        , normalized_code_type as code_type
        , replace(normalized_code,'.','') as code
        , data_source
    from __dbt__cte__cms_chronic_conditions__stg_core__condition

)

, patient_ms_drgs as (

    select
          patient_id
        , claim_id
        , claim_start_date as start_date
        , 'MS-DRG' as code_type
        , ms_drg_code as code
        , data_source
    from __dbt__cte__cms_chronic_conditions__stg_medical_claim

)

, patient_procedures as (

    select
          patient_id
        , claim_id
        , procedure_date as start_date
        , normalized_code_type as code_type
        , replace(normalized_code,'.','') as code
        , data_source
    from __dbt__cte__cms_chronic_conditions__stg_core__procedure

)

, inclusions_diagnosis as (

    select
          patient_conditions.patient_id
        , patient_conditions.claim_id
        , patient_conditions.start_date
        , patient_conditions.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_conditions
         inner join chronic_conditions
             on patient_conditions.code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'ICD-10-CM'
    and chronic_conditions.additional_logic = 'None'

)

, inclusions_ms_drg as (

    select
          patient_ms_drgs.patient_id
        , patient_ms_drgs.claim_id
        , patient_ms_drgs.start_date
        , patient_ms_drgs.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_ms_drgs
         inner join chronic_conditions
             on patient_ms_drgs.code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'MS-DRG'
    and chronic_conditions.additional_logic = 'None'

)

, inclusions_procedure as (

    select
          patient_procedures.patient_id
        , patient_procedures.claim_id
        , patient_procedures.start_date
        , patient_procedures.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_procedures
         inner join chronic_conditions
             on patient_procedures.code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system in ('ICD-10-PCS', 'HCPCS')
    and chronic_conditions.additional_logic = 'None'

)

, exclusions_diagnosis as (

    select distinct
          patient_conditions.claim_id
        , chronic_conditions.condition
    from patient_conditions
         inner join chronic_conditions
             on patient_conditions.code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Exclude'
    and chronic_conditions.code_system = 'ICD-10-CM'

)

, inclusions_unioned as (

    select * from inclusions_diagnosis
    union distinct
    select * from inclusions_procedure
    union distinct
    select * from inclusions_ms_drg

)

select distinct
      cast(inclusions_unioned.patient_id as TEXT) as patient_id
    , cast(inclusions_unioned.claim_id as TEXT) as claim_id
    , cast(inclusions_unioned.start_date as date) as start_date
    , cast(inclusions_unioned.chronic_condition_type as TEXT) as chronic_condition_type
    , cast(inclusions_unioned.condition_category as TEXT) as condition_category
    , cast(inclusions_unioned.condition as TEXT) as condition
    , cast(inclusions_unioned.data_source as TEXT) as data_source
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from inclusions_unioned
     left join exclusions_diagnosis
         on inclusions_unioned.claim_id = exclusions_diagnosis.claim_id
         and inclusions_unioned.condition = exclusions_diagnosis.condition
where exclusions_diagnosis.claim_id is null