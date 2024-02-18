
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_normalized_input_present_on_admit_normalize__dbt_tmp"
  
    as (
      


with  __dbt__cte__normalized_input__stg_medical_claim as (


select
      claim_id
    , claim_line_number
    , claim_type
    , patient_id
    , member_id
    , payer
    , plan
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , admission_date
    , discharge_date
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , place_of_service_code
    , bill_type_code
    , ms_drg_code
    , apr_drg_code
    , revenue_center_code
    , service_unit_quantity
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , rendering_npi
    , billing_npi
    , facility_npi
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , total_cost_amount
    , diagnosis_code_type
    , diagnosis_code_1
    , diagnosis_code_2
    , diagnosis_code_3
    , diagnosis_code_4
    , diagnosis_code_5
    , diagnosis_code_6
    , diagnosis_code_7
    , diagnosis_code_8
    , diagnosis_code_9
    , diagnosis_code_10
    , diagnosis_code_11
    , diagnosis_code_12
    , diagnosis_code_13
    , diagnosis_code_14
    , diagnosis_code_15
    , diagnosis_code_16
    , diagnosis_code_17
    , diagnosis_code_18
    , diagnosis_code_19
    , diagnosis_code_20
    , diagnosis_code_21
    , diagnosis_code_22
    , diagnosis_code_23
    , diagnosis_code_24
    , diagnosis_code_25
    , diagnosis_poa_1
    , diagnosis_poa_2
    , diagnosis_poa_3
    , diagnosis_poa_4
    , diagnosis_poa_5
    , diagnosis_poa_6
    , diagnosis_poa_7
    , diagnosis_poa_8
    , diagnosis_poa_9
    , diagnosis_poa_10
    , diagnosis_poa_11
    , diagnosis_poa_12
    , diagnosis_poa_13
    , diagnosis_poa_14
    , diagnosis_poa_15
    , diagnosis_poa_16
    , diagnosis_poa_17
    , diagnosis_poa_18
    , diagnosis_poa_19
    , diagnosis_poa_20
    , diagnosis_poa_21
    , diagnosis_poa_22
    , diagnosis_poa_23
    , diagnosis_poa_24
    , diagnosis_poa_25
    , procedure_code_type
    , procedure_code_1
    , procedure_code_2
    , procedure_code_3
    , procedure_code_4
    , procedure_code_5
    , procedure_code_6
    , procedure_code_7
    , procedure_code_8
    , procedure_code_9
    , procedure_code_10
    , procedure_code_11
    , procedure_code_12
    , procedure_code_13
    , procedure_code_14
    , procedure_code_15
    , procedure_code_16
    , procedure_code_17
    , procedure_code_18
    , procedure_code_19
    , procedure_code_20
    , procedure_code_21
    , procedure_code_22
    , procedure_code_23
    , procedure_code_24
    , procedure_code_25
    , procedure_date_1
    , procedure_date_2
    , procedure_date_3
    , procedure_date_4
    , procedure_date_5
    , procedure_date_6
    , procedure_date_7
    , procedure_date_8
    , procedure_date_9
    , procedure_date_10
    , procedure_date_11
    , procedure_date_12
    , procedure_date_13
    , procedure_date_14
    , procedure_date_15
    , procedure_date_16
    , procedure_date_17
    , procedure_date_18
    , procedure_date_19
    , procedure_date_20
    , procedure_date_21
    , procedure_date_22
    , procedure_date_23
    , procedure_date_24
    , procedure_date_25
    , data_source
from "synthea"."tuva_input"."medical_claim"
), pivot_poa as(
    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_1'  as diagnosis_column
        ,  diagnosis_poa_1  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_2'  as diagnosis_column
        ,  diagnosis_poa_2  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_3'  as diagnosis_column
        ,  diagnosis_poa_3  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_4'  as diagnosis_column
        ,  diagnosis_poa_4  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_5'  as diagnosis_column
        ,  diagnosis_poa_5  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_6'  as diagnosis_column
        ,  diagnosis_poa_6  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_7'  as diagnosis_column
        ,  diagnosis_poa_7  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_8'  as diagnosis_column
        ,  diagnosis_poa_8  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_9'  as diagnosis_column
        ,  diagnosis_poa_9  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_10'  as diagnosis_column
        ,  diagnosis_poa_10  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_11'  as diagnosis_column
        ,  diagnosis_poa_11  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_12'  as diagnosis_column
        ,  diagnosis_poa_12  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_13'  as diagnosis_column
        ,  diagnosis_poa_13  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_14'  as diagnosis_column
        ,  diagnosis_poa_14  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_15'  as diagnosis_column
        ,  diagnosis_poa_15  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_16'  as diagnosis_column
        ,  diagnosis_poa_16  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_17'  as diagnosis_column
        ,  diagnosis_poa_17  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_18'  as diagnosis_column
        ,  diagnosis_poa_18  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_19'  as diagnosis_column
        ,  diagnosis_poa_19  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_20'  as diagnosis_column
        ,  diagnosis_poa_20  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_21'  as diagnosis_column
        ,  diagnosis_poa_21  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_22'  as diagnosis_column
        ,  diagnosis_poa_22  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_23'  as diagnosis_column
        ,  diagnosis_poa_24  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_24'  as diagnosis_column
        ,  diagnosis_poa_24  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_25'  as diagnosis_column
        ,  diagnosis_poa_25  as present_on_admit_code
    from __dbt__cte__normalized_input__stg_medical_claim
)

select
    claim_id
    , data_source
    , diagnosis_column
    , poa.present_on_admit_code as normalized_present_on_admit_code
    , count(*) as present_on_admit_occurrence_count
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from pivot_poa piv
left join "synthea"."terminology"."present_on_admission" poa
    on replace(piv.present_on_admit_code,'.','') = poa.present_on_admit_code
where claim_type = 'institutional'
group by 
    claim_id
    , data_source
    , diagnosis_column
    , poa.present_on_admit_code
    );
  
  