
  
    
    

    create  table
      "synthea"."quality_measures"."_int_nqf2372_exclude_dementia__dbt_tmp"
  
    as (
      

/*
    Patients greater than or equal to 66 with at least one claim/encounter for frailty
    during the measurement period AND a dispensed medication for dementia during the measurement period
    or year prior to measurement period
*/

with  __dbt__cte__quality_measures__stg_core__medication as (


select
      patient_id
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."medication"


),  __dbt__cte__quality_measures__stg_pharmacy_claim as (


select
      patient_id
    , dispensing_date
    , ndc_code
    , paid_date
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."pharmacy_claim"


), patients_with_frailty as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , exclusion_date
        , exclusion_reason
    from "synthea"."quality_measures"."_int_nqf2372__frailty"

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from "synthea"."quality_measures"."_value_set_value_sets"
    where concept_name in (
        'Dementia Medications'
    )

)

, medications as (

    select
          patient_id
        , dispensing_date
        , source_code_type
        , source_code
        , ndc_code
        , rxnorm_code
    from __dbt__cte__quality_measures__stg_core__medication

)

, pharmacy_claim as (

    select
          patient_id
        , dispensing_date
        , ndc_code
        , paid_date
    from __dbt__cte__quality_measures__stg_pharmacy_claim

)

, medication_exclusions as (

    select
          medications.patient_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.ndc_code = exclusion_codes.code
    where exclusion_codes.code_system = 'ndc'

    union all

    select
          medications.patient_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.rxnorm_code = exclusion_codes.code
    where exclusion_codes.code_system = 'rxnorm'

    union all

    select
          medications.patient_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.source_code = exclusion_codes.code
            and medications.source_code_type = exclusion_codes.code_system

)

, pharmacy_claim_exclusions as (

    select
          pharmacy_claim.patient_id
        , pharmacy_claim.dispensing_date
        , pharmacy_claim.ndc_code
        , pharmacy_claim.paid_date
        , exclusion_codes.concept_name
    from pharmacy_claim
         inner join exclusion_codes
            on pharmacy_claim.ndc_code = exclusion_codes.code
    where exclusion_codes.code_system = 'ndc'

)

, frailty_with_dementia as (

    select
          patients_with_frailty.patient_id
        , patients_with_frailty.exclusion_date
        , patients_with_frailty.exclusion_reason
            || ' with '
            || pharmacy_claim_exclusions.concept_name
          as exclusion_reason
    from patients_with_frailty
         inner join pharmacy_claim_exclusions
            on patients_with_frailty.patient_id = pharmacy_claim_exclusions.patient_id
    where (
        pharmacy_claim_exclusions.dispensing_date
            between 

    date_add(patients_with_frailty.performance_period_begin, interval (-1) year)


            and patients_with_frailty.performance_period_end
        or pharmacy_claim_exclusions.paid_date
            between 

    date_add(patients_with_frailty.performance_period_begin, interval (-1) year)


            and patients_with_frailty.performance_period_end
    )

    union all

    select
          patients_with_frailty.patient_id
        , medication_exclusions.dispensing_date as exclusion_date
        , patients_with_frailty.exclusion_reason
            || ' with '
            || medication_exclusions.concept_name
          as exclusion_reason
    from patients_with_frailty
         inner join medication_exclusions
         on patients_with_frailty.patient_id = medication_exclusions.patient_id
    where medication_exclusions.dispensing_date
        between 

    date_add(patients_with_frailty.performance_period_begin, interval (-1) year)


        and patients_with_frailty.performance_period_end

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from frailty_with_dementia
    );
  
  