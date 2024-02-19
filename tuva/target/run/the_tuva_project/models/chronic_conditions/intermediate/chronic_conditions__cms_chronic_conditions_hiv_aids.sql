
  
    
    

    create  table
      "synthea"."chronic_conditions"."_int_cms_chronic_condition_hiv_aids__dbt_tmp"
  
    as (
      with  __dbt__cte__cms_chronic_conditions__stg_core__condition as (


select
      claim_id
    , patient_id
    , recorded_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."condition"
),  __dbt__cte__cms_chronic_conditions__stg_medical_claim as (


select
      claim_id
    , patient_id
    , claim_start_date
    , ms_drg_code
    , data_source
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."tuva_input"."medical_claim"
), chronic_conditions as (

    select * from "synthea"."chronic_conditions"."_value_set_cms_chronic_conditions_hierarchy"
    where condition = 'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)'

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

/*
    Exception logic: a claim with the diagnosis code R75 requires a second
    qualifying claim that is not R75 (a screening code)

    This CTE excludes encounters with the exception code. Those encounters
    will be evaluated separately.
*/
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
    and chronic_conditions.code <> 'R75'

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

)

/*
    Exception logic: a claim with the diagnosis code R75 requires a second
    qualifying claim that is not R75 (a screening code)

    This CTE includes encounters with the exception code only where that
    patient has another encounter that is not R75.
*/
, exception_diagnosis as (

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
         inner join inclusions_diagnosis
             on patient_conditions.patient_id = inclusions_diagnosis.patient_id
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'ICD-10-CM'
    and chronic_conditions.code = 'R75'

)

, inclusions_unioned as (

    select * from inclusions_diagnosis
    union distinct
    select * from inclusions_ms_drg
    union distinct
    select * from exception_diagnosis

)

select distinct
      cast(inclusions_unioned.patient_id as TEXT) as patient_id
    , cast(inclusions_unioned.claim_id as TEXT) as claim_id
    , cast(inclusions_unioned.start_date as date) as start_date
    , cast(inclusions_unioned.chronic_condition_type as TEXT) as chronic_condition_type
    , cast(inclusions_unioned.condition_category as TEXT) as condition_category
    , cast(inclusions_unioned.condition as TEXT) as condition
    , cast(inclusions_unioned.data_source as TEXT) as data_source
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from inclusions_unioned
    );
  
  