
  
    
    

    create  table
      "synthea"."hcc_suspecting"."summary__dbt_tmp"
  
    as (
      

with  __dbt__cte__hcc_suspecting__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."patient"
), patients as (

    select
          patient_id
        , sex
        , birth_date
        , floor(
        (
        ((current_date)::date - (birth_date)::date)
     * 24 + date_part('hour', (current_date)::timestamp) - date_part('hour', (birth_date)::timestamp))
     / 8766.0) as age
    from __dbt__cte__hcc_suspecting__stg_core__patient
    where death_date is null

)

, suspecting_list as (

      select
          patient_id
        , count(*) as gaps
    from "synthea"."hcc_suspecting"."list"
    group by patient_id

)

, joined as (

    select
          patients.patient_id
        , patients.sex
        , patients.birth_date
        , patients.age
        , suspecting_list.gaps
    from patients
         inner join suspecting_list
         on patients.patient_id = suspecting_list.patient_id

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(sex as TEXT) as patient_sex
        , cast(birth_date as date) as patient_birth_date
        , cast(age as integer) as patient_age
        , cast(gaps as integer) as suspecting_gaps
    from joined

)

select
      patient_id
    , patient_sex
    , patient_birth_date
    , patient_age
    , suspecting_gaps
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from add_data_types
    );
  
  