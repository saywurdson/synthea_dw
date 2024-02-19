-- depends_on: "synthea"."data_quality"."claims_preprocessing_summary"



-- *************************************************
-- This dbt model creates the medical_claim table
-- in core. It adds these 4 fields to the input layer
-- medical claim table:
--      encounter_id
--      service_category_1
--      service_category_2
-- *************************************************


select
    cast(med.claim_id as TEXT ) as claim_id
    , cast(med.claim_line_number as integer ) as claim_line_number
    , cast(coalesce(ap.encounter_id,ed.encounter_id) as TEXT ) as encounter_id 
    , cast(med.claim_type as TEXT ) as claim_type
    , cast(med.patient_id as TEXT ) as patient_id
    , cast(med.member_id as TEXT ) as member_id
    , cast(med.payer as TEXT ) as payer
    , cast(med.plan as TEXT ) as plan
    , try_cast( med.claim_start_date as date ) as claim_start_date
    , try_cast( med.claim_end_date as date ) as claim_end_date
    , try_cast( med.claim_line_start_date as date ) as claim_line_start_date
    , try_cast( med.claim_line_end_date as date ) as claim_line_end_date
    , try_cast( med.admission_date as date ) as admission_date
    , try_cast( med.discharge_date as date ) as discharge_date
    , cast(srv_group.service_category_1 as TEXT ) as service_category_1
    , cast(srv_group.service_category_2 as TEXT ) as service_category_2
    , cast(med.admit_source_code as TEXT ) as admit_source_code
    , cast(med.admit_type_code as TEXT ) as admit_type_code
    , cast(med.discharge_disposition_code as TEXT ) as discharge_disposition_code
    , cast(med.place_of_service_code as TEXT ) as place_of_service_code
    , cast(med.bill_type_code as TEXT ) as bill_type_code
    , cast(med.ms_drg_code as TEXT ) as ms_drg_code
    , cast(med.apr_drg_code as TEXT ) as apr_drg_code
    , cast(med.revenue_center_code as TEXT ) as revenue_center_code
    , cast(med.service_unit_quantity as integer ) as service_unit_quantity
    , cast(med.hcpcs_code as TEXT ) as hcpcs_code
    , cast(med.hcpcs_modifier_1 as TEXT ) as hcpcs_modifier_1
    , cast(med.hcpcs_modifier_2 as TEXT ) as hcpcs_modifier_2
    , cast(med.hcpcs_modifier_3 as TEXT ) as hcpcs_modifier_3
    , cast(med.hcpcs_modifier_4 as TEXT ) as hcpcs_modifier_4
    , cast(med.hcpcs_modifier_5 as TEXT ) as hcpcs_modifier_5
    , cast(med.rendering_npi as TEXT ) as rendering_npi
    , cast(med.billing_npi as TEXT ) as billing_npi
    , cast(med.facility_npi as TEXT ) as facility_npi
    , try_cast( med.paid_date as date ) as paid_date
    , cast(med.paid_amount as numeric(28,6) ) as paid_amount
    , cast(med.allowed_amount as numeric(28,6) ) as allowed_amount
    , cast(med.charge_amount as numeric(28,6) ) as charge_amount
    , cast(med.coinsurance_amount as numeric(28,6) ) as coinsurance_amount
    , cast(med.copayment_amount as numeric(28,6) ) as copayment_amount
    , cast(med.deductible_amount as numeric(28,6) ) as deductible_amount
    , cast(med.total_cost_amount as numeric(28,6) ) as total_cost_amount
    , cast(med.data_source as TEXT ) as data_source
    , cast('2024-02-18 21:13:49.400698+00:00' as timestamp ) as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim" med
left join "synthea"."claims_preprocessing"."service_category_grouper" srv_group
    on med.claim_id = srv_group.claim_id
    and med.claim_line_number = srv_group.claim_line_number
left join "synthea"."claims_preprocessing"."_int_acute_inpatient_encounter_id" ap
    on med.claim_id = ap.claim_id
    and med.claim_line_number = ap.claim_line_number
left join "synthea"."claims_preprocessing"."_int_emergency_department_encounter_id" ed
    on med.claim_id = ed.claim_id
    and med.claim_line_number = ed.claim_line_number