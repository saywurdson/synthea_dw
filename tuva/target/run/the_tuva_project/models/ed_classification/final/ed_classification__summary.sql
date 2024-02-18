
  
    
    

    create  table
      "synthea"."ed_classification"."summary__dbt_tmp"
  
    as (
      


with __dbt__cte__ed_classification__stg_patient as (


select
    patient_id
    , sex
    , birth_date
    , race
    , state
    , zip_code
    , latitude
    , longitude
from "synthea"."core"."patient"
) select
    class.encounter_id
    , cat.classification_name as ed_classification_description
    , cat.classification_order as ed_classification_order
    , class.patient_id
    , class.encounter_end_date
    , cast(date_part('year', class.encounter_end_date) as TEXT) 
        || right('0'||cast(date_part('month', class.encounter_end_date) as TEXT),2) 
    as year_month
    , class.primary_diagnosis_code
    , class.primary_diagnosis_description
    , class.paid_amount
    , class.allowed_amount
    , class.charge_amount
    , class.facility_npi
    , fac_prov.provider_organization_name as facility_name
    , practice_state as facility_state
    , practice_city as facility_city
    , practice_zip_code as facility_zip_code
--     , null as facility_latitude
--     , null as facility_longitude
    , pat.sex as patient_sex
    , floor(
        (
        ((class.encounter_end_date)::date - (pat.birth_date)::date)
     * 24 + date_part('hour', (class.encounter_end_date)::timestamp) - date_part('hour', (pat.birth_date)::timestamp))
     / 8766.0) as patient_age
    , zip_code as patient_zip_code
    , latitude as patient_latitude
    , longitude as patient_longitude
    , race as patient_race
from "synthea"."ed_classification"."_int_filter_encounter_with_classification" class
inner join "synthea"."ed_classification"."_value_set_categories" cat
    using(classification)
left join "synthea"."terminology"."provider" fac_prov 
    on class.facility_npi = fac_prov.npi
left join __dbt__cte__ed_classification__stg_patient pat
    on class.patient_id = pat.patient_id
    );
  
  