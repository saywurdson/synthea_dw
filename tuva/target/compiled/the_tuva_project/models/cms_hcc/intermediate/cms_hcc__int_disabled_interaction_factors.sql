

with demographics as (

    select
          patient_id
        , enrollment_status
        , institutional_status
        , model_version
        , payment_year
    from "synthea"."cms_hcc"."_int_demographic_factors"

)

, hcc_hierarchy as (

    select
          patient_id
        , hcc_code
        , model_version
    from "synthea"."cms_hcc"."_int_hcc_hierarchy"

)

, seed_interaction_factors as (

    select
          model_version
        , factor_type
        , enrollment_status
        , institutional_status
        , short_name
        , description
        , hcc_code
        , coefficient
    from "synthea"."cms_hcc"."_value_set_disabled_interaction_factors"

)

, demographics_with_hccs as (

    select
          demographics.patient_id
        , demographics.enrollment_status
        , demographics.institutional_status
        , demographics.model_version
        , demographics.payment_year
        , hcc_hierarchy.hcc_code
    from demographics
        inner join hcc_hierarchy
            on demographics.patient_id = hcc_hierarchy.patient_id
            and demographics.model_version = hcc_hierarchy.model_version

)

, interactions as (

    select
          demographics_with_hccs.patient_id
        , demographics_with_hccs.model_version
        , demographics_with_hccs.payment_year
        , seed_interaction_factors.factor_type
        , seed_interaction_factors.description
        , seed_interaction_factors.coefficient
    from demographics_with_hccs
        inner join seed_interaction_factors
            on demographics_with_hccs.enrollment_status = seed_interaction_factors.enrollment_status
            and demographics_with_hccs.institutional_status = seed_interaction_factors.institutional_status
            and demographics_with_hccs.hcc_code = seed_interaction_factors.hcc_code
            and demographics_with_hccs.model_version = seed_interaction_factors.model_version

)

, add_data_types as (

select
      cast(patient_id as TEXT) as patient_id
    , cast(description as TEXT) as description
    , round(cast(coefficient as numeric(28,6)),3) as coefficient
    , cast(factor_type as TEXT) as factor_type
    , cast(model_version as TEXT) as model_version
    , cast(payment_year as integer) as payment_year
from interactions

)

select
      patient_id
    , description
    , coefficient
    , factor_type
    , model_version
    , payment_year
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from add_data_types