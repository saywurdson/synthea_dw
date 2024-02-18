

with institutional_header_duplicates as (

 
        select
              claim_id
            , data_source
            , 'claim_id' as column_checked
            , count(distinct claim_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct claim_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'claim_type' as column_checked
            , count(distinct claim_type) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct claim_type) > 1
        union all
        select
              claim_id
            , data_source
            , 'patient_id' as column_checked
            , count(distinct patient_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct patient_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'member_id' as column_checked
            , count(distinct member_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct member_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'payer' as column_checked
            , count(distinct payer) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct payer) > 1
        union all
        select
              claim_id
            , data_source
            , 'plan' as column_checked
            , count(distinct plan) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct plan) > 1
        union all
        select
              claim_id
            , data_source
            , 'claim_start_date' as column_checked
            , count(distinct claim_start_date) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct claim_start_date) > 1
        union all
        select
              claim_id
            , data_source
            , 'claim_end_date' as column_checked
            , count(distinct claim_end_date) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct claim_end_date) > 1
        union all
        select
              claim_id
            , data_source
            , 'admission_date' as column_checked
            , count(distinct admission_date) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct admission_date) > 1
        union all
        select
              claim_id
            , data_source
            , 'discharge_date' as column_checked
            , count(distinct discharge_date) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct discharge_date) > 1
        union all
        select
              claim_id
            , data_source
            , 'admit_source_code' as column_checked
            , count(distinct admit_source_code) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct admit_source_code) > 1
        union all
        select
              claim_id
            , data_source
            , 'admit_type_code' as column_checked
            , count(distinct admit_type_code) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct admit_type_code) > 1
        union all
        select
              claim_id
            , data_source
            , 'discharge_disposition_code' as column_checked
            , count(distinct discharge_disposition_code) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct discharge_disposition_code) > 1
        union all
        select
              claim_id
            , data_source
            , 'bill_type_code' as column_checked
            , count(distinct bill_type_code) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct bill_type_code) > 1
        union all
        select
              claim_id
            , data_source
            , 'ms_drg_code' as column_checked
            , count(distinct ms_drg_code) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct ms_drg_code) > 1
        union all
        select
              claim_id
            , data_source
            , 'facility_npi' as column_checked
            , count(distinct facility_npi) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct facility_npi) > 1
        union all
        select
              claim_id
            , data_source
            , 'billing_npi' as column_checked
            , count(distinct billing_npi) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct billing_npi) > 1
        union all
        select
              claim_id
            , data_source
            , 'rendering_npi' as column_checked
            , count(distinct rendering_npi) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct rendering_npi) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_type' as column_checked
            , count(distinct diagnosis_code_type) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_type) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_1' as column_checked
            , count(distinct diagnosis_code_1) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_1) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_2' as column_checked
            , count(distinct diagnosis_code_2) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_2) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_3' as column_checked
            , count(distinct diagnosis_code_3) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_3) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_4' as column_checked
            , count(distinct diagnosis_code_4) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_4) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_5' as column_checked
            , count(distinct diagnosis_code_5) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_5) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_6' as column_checked
            , count(distinct diagnosis_code_6) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_6) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_7' as column_checked
            , count(distinct diagnosis_code_7) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_7) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_8' as column_checked
            , count(distinct diagnosis_code_8) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_8) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_9' as column_checked
            , count(distinct diagnosis_code_9) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_9) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_10' as column_checked
            , count(distinct diagnosis_code_10) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_10) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_11' as column_checked
            , count(distinct diagnosis_code_11) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_11) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_12' as column_checked
            , count(distinct diagnosis_code_12) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_12) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_13' as column_checked
            , count(distinct diagnosis_code_13) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_13) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_14' as column_checked
            , count(distinct diagnosis_code_14) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_14) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_15' as column_checked
            , count(distinct diagnosis_code_15) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_15) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_16' as column_checked
            , count(distinct diagnosis_code_16) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_16) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_17' as column_checked
            , count(distinct diagnosis_code_17) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_17) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_18' as column_checked
            , count(distinct diagnosis_code_18) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_18) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_19' as column_checked
            , count(distinct diagnosis_code_19) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_19) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_20' as column_checked
            , count(distinct diagnosis_code_20) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_20) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_21' as column_checked
            , count(distinct diagnosis_code_21) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_21) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_22' as column_checked
            , count(distinct diagnosis_code_22) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_22) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_23' as column_checked
            , count(distinct diagnosis_code_23) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_23) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_24' as column_checked
            , count(distinct diagnosis_code_24) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_24) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_25' as column_checked
            , count(distinct diagnosis_code_25) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_25) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_1' as column_checked
            , count(distinct diagnosis_poa_1) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_1) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_2' as column_checked
            , count(distinct diagnosis_poa_2) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_2) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_3' as column_checked
            , count(distinct diagnosis_poa_3) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_3) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_4' as column_checked
            , count(distinct diagnosis_poa_4) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_4) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_5' as column_checked
            , count(distinct diagnosis_poa_5) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_5) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_6' as column_checked
            , count(distinct diagnosis_poa_6) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_6) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_7' as column_checked
            , count(distinct diagnosis_poa_7) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_7) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_8' as column_checked
            , count(distinct diagnosis_poa_8) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_8) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_9' as column_checked
            , count(distinct diagnosis_poa_9) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_9) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_10' as column_checked
            , count(distinct diagnosis_poa_10) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_10) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_11' as column_checked
            , count(distinct diagnosis_poa_11) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_11) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_12' as column_checked
            , count(distinct diagnosis_poa_12) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_12) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_13' as column_checked
            , count(distinct diagnosis_poa_13) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_13) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_14' as column_checked
            , count(distinct diagnosis_poa_14) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_14) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_15' as column_checked
            , count(distinct diagnosis_poa_15) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_15) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_16' as column_checked
            , count(distinct diagnosis_poa_16) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_16) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_17' as column_checked
            , count(distinct diagnosis_poa_17) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_17) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_18' as column_checked
            , count(distinct diagnosis_poa_18) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_18) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_19' as column_checked
            , count(distinct diagnosis_poa_19) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_19) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_20' as column_checked
            , count(distinct diagnosis_poa_20) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_20) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_21' as column_checked
            , count(distinct diagnosis_poa_21) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_21) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_22' as column_checked
            , count(distinct diagnosis_poa_22) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_22) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_23' as column_checked
            , count(distinct diagnosis_poa_23) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_23) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_24' as column_checked
            , count(distinct diagnosis_poa_24) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_24) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_poa_25' as column_checked
            , count(distinct diagnosis_poa_25) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_poa_25) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_type' as column_checked
            , count(distinct procedure_code_type) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_type) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_1' as column_checked
            , count(distinct procedure_code_1) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_1) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_2' as column_checked
            , count(distinct procedure_code_2) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_2) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_3' as column_checked
            , count(distinct procedure_code_3) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_3) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_4' as column_checked
            , count(distinct procedure_code_4) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_4) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_5' as column_checked
            , count(distinct procedure_code_5) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_5) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_6' as column_checked
            , count(distinct procedure_code_6) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_6) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_7' as column_checked
            , count(distinct procedure_code_7) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_7) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_8' as column_checked
            , count(distinct procedure_code_8) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_8) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_9' as column_checked
            , count(distinct procedure_code_9) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_9) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_10' as column_checked
            , count(distinct procedure_code_10) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_10) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_11' as column_checked
            , count(distinct procedure_code_11) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_11) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_12' as column_checked
            , count(distinct procedure_code_12) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_12) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_13' as column_checked
            , count(distinct procedure_code_13) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_13) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_14' as column_checked
            , count(distinct procedure_code_14) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_14) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_15' as column_checked
            , count(distinct procedure_code_15) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_15) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_16' as column_checked
            , count(distinct procedure_code_16) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_16) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_17' as column_checked
            , count(distinct procedure_code_17) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_17) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_18' as column_checked
            , count(distinct procedure_code_18) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_18) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_19' as column_checked
            , count(distinct procedure_code_19) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_19) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_20' as column_checked
            , count(distinct procedure_code_20) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_20) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_21' as column_checked
            , count(distinct procedure_code_21) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_21) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_22' as column_checked
            , count(distinct procedure_code_22) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_22) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_23' as column_checked
            , count(distinct procedure_code_23) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_23) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_24' as column_checked
            , count(distinct procedure_code_24) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_24) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_code_25' as column_checked
            , count(distinct procedure_code_25) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_code_25) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_1' as column_checked
            , count(distinct procedure_date_1) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_1) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_2' as column_checked
            , count(distinct procedure_date_2) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_2) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_3' as column_checked
            , count(distinct procedure_date_3) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_3) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_4' as column_checked
            , count(distinct procedure_date_4) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_4) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_5' as column_checked
            , count(distinct procedure_date_5) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_5) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_6' as column_checked
            , count(distinct procedure_date_6) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_6) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_7' as column_checked
            , count(distinct procedure_date_7) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_7) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_8' as column_checked
            , count(distinct procedure_date_8) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_8) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_9' as column_checked
            , count(distinct procedure_date_9) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_9) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_10' as column_checked
            , count(distinct procedure_date_10) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_10) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_11' as column_checked
            , count(distinct procedure_date_11) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_11) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_12' as column_checked
            , count(distinct procedure_date_12) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_12) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_13' as column_checked
            , count(distinct procedure_date_13) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_13) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_14' as column_checked
            , count(distinct procedure_date_14) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_14) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_15' as column_checked
            , count(distinct procedure_date_15) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_15) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_16' as column_checked
            , count(distinct procedure_date_16) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_16) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_17' as column_checked
            , count(distinct procedure_date_17) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_17) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_18' as column_checked
            , count(distinct procedure_date_18) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_18) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_19' as column_checked
            , count(distinct procedure_date_19) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_19) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_20' as column_checked
            , count(distinct procedure_date_20) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_20) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_21' as column_checked
            , count(distinct procedure_date_21) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_21) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_22' as column_checked
            , count(distinct procedure_date_22) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_22) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_23' as column_checked
            , count(distinct procedure_date_23) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_23) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_24' as column_checked
            , count(distinct procedure_date_24) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_24) > 1
        union all
        select
              claim_id
            , data_source
            , 'procedure_date_25' as column_checked
            , count(distinct procedure_date_25) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct procedure_date_25) > 1
        union all
        select
              claim_id
            , data_source
            , 'data_source' as column_checked
            , count(distinct data_source) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'institutional'
        group by
              claim_id
            , data_source
        having count(distinct data_source) > 1
        

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
        , claim_type
    from "synthea"."data_quality"."_value_set_test_catalog"

)

select
      test_catalog.source_table
    , 'institutional' as claim_type
    , 'claim_id' as grain
    , institutional_header_duplicates.claim_id
    , institutional_header_duplicates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from institutional_header_duplicates
     left join test_catalog
       on test_catalog.test_name = institutional_header_duplicates.column_checked||' non-unique'
       and test_catalog.source_table = 'normalized_input__medical_claim'
       and test_catalog.claim_type = 'institutional'
group by 
      institutional_header_duplicates.claim_id
    , institutional_header_duplicates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test