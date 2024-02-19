
  
    
    

    create  table
      "synthea"."cms_hcc"."_int_disease_interaction_factors__dbt_tmp"
  
    as (
      

with demographics as (

    select
          patient_id
        , enrollment_status
        , gender
        , age_group
        , medicaid_status
        , dual_status
        , orec
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
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , short_name
        , description
        , hcc_code_1
        , hcc_code_2
        , coefficient
    from "synthea"."cms_hcc"."_value_set_disease_interaction_factors"

)

, demographics_with_hccs as (

    select
          demographics.patient_id
        , demographics.enrollment_status
        , demographics.medicaid_status
        , demographics.dual_status
        , demographics.orec
        , demographics.institutional_status
        , demographics.model_version
        , demographics.payment_year
        , hcc_hierarchy.hcc_code
    from demographics
        inner join hcc_hierarchy
            on demographics.patient_id = hcc_hierarchy.patient_id
            and demographics.model_version = hcc_hierarchy.model_version

)

, demographics_with_interactions as (

    select
          demographics_with_hccs.patient_id
        , demographics_with_hccs.model_version
        , demographics_with_hccs.payment_year
        , interactions_code_1.factor_type
        , interactions_code_1.description
        , interactions_code_1.hcc_code_1
        , interactions_code_1.hcc_code_2
        , interactions_code_1.coefficient
    from demographics_with_hccs
        inner join seed_interaction_factors as interactions_code_1
            on demographics_with_hccs.enrollment_status = interactions_code_1.enrollment_status
            and demographics_with_hccs.medicaid_status = interactions_code_1.medicaid_status
            and demographics_with_hccs.dual_status = interactions_code_1.dual_status
            and demographics_with_hccs.orec = interactions_code_1.orec
            and demographics_with_hccs.institutional_status = interactions_code_1.institutional_status
            and demographics_with_hccs.hcc_code = interactions_code_1.hcc_code_1
            and demographics_with_hccs.model_version = interactions_code_1.model_version

)

, disease_interactions as (

    select
          demographics_with_interactions.patient_id
        , demographics_with_interactions.factor_type
        , demographics_with_interactions.hcc_code_1
        , demographics_with_interactions.hcc_code_2
        , demographics_with_interactions.description
        , demographics_with_interactions.coefficient
        , demographics_with_interactions.model_version
        , demographics_with_interactions.payment_year
    from demographics_with_interactions
        inner join demographics_with_hccs as interactions_code_2
            on demographics_with_interactions.patient_id = interactions_code_2.patient_id
            and demographics_with_interactions.hcc_code_2 = interactions_code_2.hcc_code
            and demographics_with_interactions.model_version = interactions_code_2.model_version
)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(hcc_code_1 as TEXT) as hcc_code_1
        , cast(hcc_code_2 as TEXT) as hcc_code_2
        , cast(description as TEXT) as description
        , round(cast(coefficient as numeric(28,6)),3) as coefficient
        , cast(factor_type as TEXT) as factor_type
        , cast(model_version as TEXT) as model_version
        , cast(payment_year as integer) as payment_year
    from disease_interactions

)

select
      patient_id
    , hcc_code_1
    , hcc_code_2
    , description
    , coefficient
    , factor_type
    , model_version
    , payment_year
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from add_data_types
    );
  
  