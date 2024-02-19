with  __dbt__cte__cms_chronic_conditions__stg_core__condition as (


select
      claim_id
    , patient_id
    , recorded_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."condition"
),  __dbt__cte__cms_chronic_conditions__stg_pharmacy_claim as (


select
      claim_id
    , patient_id
    , paid_date
    , ndc_code
    , data_source
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."tuva_input"."pharmacy_claim"
),  __dbt__cte__cms_chronic_conditions__stg_core__procedure as (


select
      claim_id
    , patient_id
    , procedure_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."procedure"
), chronic_conditions as (

    select * from "synthea"."chronic_conditions"."_value_set_cms_chronic_conditions_hierarchy"
    where condition = 'Opioid Use Disorder (OUD)'

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

, patient_medications as (

    select
          patient_id
        , claim_id
        , paid_date as start_date
        , replace(ndc_code,'.','') as code
        , data_source
    from __dbt__cte__cms_chronic_conditions__stg_pharmacy_claim

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

)

/*
    Exclusion logic: Naltrexone NDCs are excluded if there is evidence of an
    alcohol or other drug use disorder where opioid DX is not present

    This CTE excludes medication encounters with the exception codes for
    Naltrexone. Those encounters will be evaluated separately.
*/
, inclusions_medication as (

    select
          patient_medications.patient_id
        , patient_medications.claim_id
        , patient_medications.start_date
        , patient_medications.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_medications
         inner join chronic_conditions
             on patient_medications.code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'NDC'
    and chronic_conditions.code not in ('00056001122', '00056001130', '00056001170', '00056007950', '00056008050', '00185003901', '00185003930', '00406009201', '00406009203', '00406117001', '00406117003', '00555090201', '00555090202', '00904703604', '16729008101', '16729008110', '42291063230', '43063059115', '47335032683', '47335032688', '50090286600', '50436010501', '51224020630', '51224020650', '51285027501', '51285027502', '52152010502', '52152010504', '52152010530', '54868557400', '63459030042', '63629104601', '63629104701', '65694010003', '65694010010', '65757030001', '65757030202', '68084029111', '68084029121', '68094085362', '68115068030')

)

/*
    Exclusion logic: Naltrexone NDCs are excluded if there is evidence of an
    alcohol or other drug use disorder where opioid DX is not present

    This CTE includes patients with evidence of the chronic conditions Alcohol
    Use Disorders or Drug Use Disorders.
*/
, exclusions_other_chronic_conditions as (

    select distinct patient_id
    from "synthea"."chronic_conditions"."_int_cms_chronic_condition_all"
    where condition in (
          'Alcohol Use Disorders'
        , 'Drug Use Disorders'
    )

)

/*
    Exclusion logic: Naltrexone NDCs are excluded if there is evidence of an
    alcohol or other drug use disorder where opioid DX is not present

    This CTE creates the exclusion list which consists of patients with
    medication encounters for Naltrexone having Alcohol Use Disorder or Drug
    Use Disorder and missing the Opioid Use Disorder diagnosis codes.
*/
, exclusions_medication as (
    select distinct
          patient_medications.patient_id
    from patient_medications
         inner join chronic_conditions
             on patient_medications.code = chronic_conditions.code
         inner join exclusions_other_chronic_conditions
             on patient_medications.patient_id = exclusions_other_chronic_conditions.patient_id
         left join inclusions_diagnosis
             on patient_medications.patient_id = inclusions_diagnosis.patient_id
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'NDC'
    and chronic_conditions.code in ('00056001122', '00056001130', '00056001170', '00056007950', '00056008050', '00185003901', '00185003930', '00406009201', '00406009203', '00406117001', '00406117003', '00555090201', '00555090202', '00904703604', '16729008101', '16729008110', '42291063230', '43063059115', '47335032683', '47335032688', '50090286600', '50436010501', '51224020630', '51224020650', '51285027501', '51285027502', '52152010502', '52152010504', '52152010530', '54868557400', '63459030042', '63629104601', '63629104701', '65694010003', '65694010010', '65757030001', '65757030202', '68084029111', '68084029121', '68094085362', '68115068030')
    and inclusions_diagnosis.patient_id is null

)

, inclusions_unioned as (

    select * from inclusions_diagnosis
    union distinct
    select * from inclusions_procedure
    union distinct
    select * from inclusions_medication

)

select distinct
      cast(inclusions_unioned.patient_id as TEXT) as patient_id
    , cast(inclusions_unioned.claim_id as TEXT) as claim_id
    , cast(inclusions_unioned.start_date as date) as start_date
    , cast(inclusions_unioned.chronic_condition_type as TEXT) as chronic_condition_type
    , cast(inclusions_unioned.condition_category as TEXT) as condition_category
    , cast(inclusions_unioned.condition as TEXT) as condition
    , cast(inclusions_unioned.data_source as TEXT) as data_source
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from inclusions_unioned
     left join exclusions_medication
         on inclusions_unioned.patient_id = exclusions_medication.patient_id
where exclusions_medication.patient_id is null