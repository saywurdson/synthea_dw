

with  __dbt__cte__tuva_chronic_conditions__stg_core__condition as (


select 
      patient_id
    , normalized_code
    , recorded_date
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."condition"
), condition_row_number as (

    select 
          patient_id
        , normalized_code
        , recorded_date
        , row_number() over(
            partition by
                  patient_id
                , normalized_code
            order by recorded_date asc
          ) as rn_asc
        , row_number() over(
            partition by
                  patient_id
                , normalized_code
            order by recorded_date desc
          ) as rn_desc
    from __dbt__cte__tuva_chronic_conditions__stg_core__condition

)

, patient_conditions as (

    select 
          patient_id
        , normalized_code as icd_10_cm
        , max(
            case
                when rn_asc = 1
                then recorded_date
            end
          ) as first_diagnosis_date
        , max(
            case
                when rn_desc = 1
                then recorded_date
            end
          ) as last_diagnosis_date
    from condition_row_number
    group by 
          patient_id
        , normalized_code

)

select 
      pc.patient_id
    , h.condition_family
    , h.condition
    , min(first_diagnosis_date) as first_diagnosis_date
    , max(last_diagnosis_date) as last_diagnosis_date
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."chronic_conditions"."_value_set_tuva_chronic_conditions_hierarchy" h
     inner join patient_conditions pc
        on h.icd_10_cm_code = pc.icd_10_cm
group by 
      pc.patient_id
    , h.condition_family
    , h.condition