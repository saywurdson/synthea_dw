
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_acute_inpatient_institutional_claims__dbt_tmp"
  
    as (
      

-- *************************************************
-- This dbt model gives us all acute inpatient institutional claims.
-- We have one row per claim_id (for all claim_ids belonging to
-- acute inpatient institutinal claims).
-- The number of rows in the table is equal to the number of unique
-- claim_ids (i.e. claim_id is a primary key).
-- Note that we are assuming that a claim_id is unique across
-- all people in the datset, i.e.
-- that no two people can have the same claim_id.
-- *************************************************

with  __dbt__cte__acute_inpatient__stg_medical_claim as (


select 
    claim_id
    , claim_line_number
    , patient_id
    , claim_type
    , claim_start_date
    , claim_end_date
    , admission_date
    , discharge_date
    , facility_npi
    , ms_drg_code
    , apr_drg_code
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , paid_amount
    , allowed_amount
    , charge_amount
    , diagnosis_code_type
    , diagnosis_code_1
    , data_source
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
),  __dbt__cte__acute_inpatient__stg_service_category as (



SELECT
  claim_id
, claim_type
, claim_line_number
, service_category_2
, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."service_category_grouper"
), acute_inpatient_claim_lines as (
select
  mc.patient_id,
  mc.claim_id,
  mc.claim_line_number,
  mc.claim_start_date,
  mc.claim_end_date,
  mc.admission_date,
  mc.discharge_date,
  mc.admit_source_code,
  mc.admit_type_code,
  mc.discharge_disposition_code,
  mc.facility_npi,
  mc.claim_type,
  mc.data_source
from __dbt__cte__acute_inpatient__stg_medical_claim mc
inner join __dbt__cte__acute_inpatient__stg_service_category sc
  on mc.claim_id = sc.claim_id
where mc.claim_type = 'institutional'
  and sc.service_category_2 = 'Acute Inpatient'
),

data_quality_flags as (
select
  claim_id,
-- patient_id_not_unique:
  case
    when count(distinct patient_id) > 1 then 1
    else 0
  end as patient_id_not_unique,
-- patient_id_missing:  
  case
    when max(patient_id) is null then 1
    else 0
  end as patient_id_missing,  
-- claim_start_date_not_unique:
  case
    when count(distinct claim_start_date) > 1 then 1
    else 0
  end as claim_start_date_not_unique,
-- claim_start_date_missing:  
  case
    when max(claim_start_date) is null then 1
    else 0
  end as claim_start_date_missing,
-- claim_end_date_not_unique:
  case
    when count(distinct claim_end_date) > 1 then 1
    else 0
  end as claim_end_date_not_unique,
-- claim_end_date_missing:  
  case
    when max(claim_end_date) is null then 1
    else 0
  end as claim_end_date_missing,
-- claim_start_date_after_claim_end_date:
  case
    when min(claim_start_date) > max(claim_end_date) then 1
    else 0
  end as claim_start_date_after_claim_end_date,
-- admission_date_not_unique:
  case
    when count(distinct admission_date) > 1 then 1
    else 0
  end as admission_date_not_unique,
-- admission_date_missing:  
  case
    when max(admission_date) is null then 1
    else 0
  end as admission_date_missing,
-- discharge_date_not_unique:
  case
    when count(distinct discharge_date) > 1 then 1
    else 0
  end as discharge_date_not_unique,
-- discharge_date_missing:  
  case
    when max(discharge_date) is null then 1
    else 0
  end as discharge_date_missing,
-- admission_date_after_discharge_date:
  case
    when min(admission_date) > max(discharge_date) then 1
    else 0
  end as admission_date_after_discharge_date,
-- admit_type_code_not_unique:
  case
    when count(distinct admit_type_code) > 1 then 1
    else 0
  end as admit_type_code_not_unique,
-- admit_type_code_missing:  
  case
    when max(admit_type_code) is null then 1
    else 0
  end as admit_type_code_missing,  
-- admit_source_code_not_unique:
  case
    when count(distinct admit_source_code) > 1 then 1
    else 0
  end as admit_source_code_not_unique,
-- admit_source_code_missing:  
  case
    when max(admit_source_code) is null then 1
    else 0
  end as admit_source_code_missing,  
-- discharge_disposition_code_not_unique:
  case
    when count(distinct discharge_disposition_code) > 1 then 1
    else 0
  end as discharge_disposition_code_not_unique,
-- discharge_disposition_code_missing:  
  case
    when max(discharge_disposition_code) is null then 1
    else 0
  end as discharge_disposition_code_missing,
-- facility_npi_not_unique:
  case
    when count(distinct facility_npi) > 1 then 1
    else 0
  end as facility_npi_not_unique,
-- facility_npi_missing:  
  case
    when max(facility_npi) is null then 1
    else 0
  end as facility_npi_missing,
-- claim_type_not_unique:
  case
    when count(distinct claim_type) > 1 then 1
    else 0
  end as claim_type_not_unique,
-- claim_type_missing:  
  case
    when max(claim_type) is null then 1
    else 0
  end as claim_type_missing,
-- claim_type_not_institutional:
  case
    when max(claim_type) <> 'institutional'
     and min(claim_type) <> 'institutional' then 1
    else 0
  end as claim_type_not_institutional
from acute_inpatient_claim_lines
group by claim_id
),


header_level_values as (
select
  claim_id,
  max(patient_id) as patient_id,
  min(claim_start_date) as claim_start_date,
  max(claim_end_date) as claim_end_date,
  min(admission_date) as admission_date,
  max(discharge_date) as discharge_date,
  max(admit_source_code) as admit_source_code,
  max(admit_type_code) as admit_type_code,
  max(discharge_disposition_code) as discharge_disposition_code,
  max(facility_npi) as facility_npi,
  max(claim_type) as claim_type,
  coalesce(min(admission_date),
           min(claim_start_date)) as start_date,
  coalesce(max(discharge_date),
           max(claim_end_date)) as end_date,
  case
    when min(admission_date) is not null then 'admission_date'
    when min(claim_start_date) is not null then 'claim_start_date'
    else null
  end as date_used_as_start_date,
  case
    when max(discharge_date) is not null then 'discharge_date'
    when max(claim_end_date) is not null then 'claim_end_date'
    else null
  end as date_used_as_end_date,
  data_source
from acute_inpatient_claim_lines
group by claim_id, data_source
)

select
  h.patient_id as patient_id,
  h.claim_id as claim_id,  
  h.claim_start_date as claim_start_date,
  h.claim_end_date as claim_end_date,
  h.admission_date as admission_date,
  h.discharge_date as discharge_date,
  h.admit_source_code as admit_source_code,
  h.admit_type_code as admit_type_code,
  h.discharge_disposition_code as discharge_disposition_code,
  h.facility_npi as facility_npi,
  h.claim_type as claim_type,
  h.start_date as start_date,
  h.end_date as end_date,
  h.date_used_as_start_date,
  h.date_used_as_end_date, 

  case
    when
      ( (dq.patient_id_not_unique = 1) or
        (dq.patient_id_missing = 1) or
        (dq.discharge_disposition_code_not_unique = 1) or
        (dq.discharge_disposition_code_missing = 1) or
        (dq.facility_npi_not_unique = 1) or
        (dq.facility_npi_missing = 1) or
        (h.date_used_as_start_date is null) or
	(h.date_used_as_end_date is null) or
	(h.start_date > h.end_date) ) then 1
    else 0
  end as dq_problem,

  case
    when
      ( (dq.claim_start_date_not_unique = 1) or
	(dq.claim_start_date_missing = 1) or
        (dq.claim_end_date_not_unique = 1) or
        (dq.claim_end_date_missing = 1) or
        (dq.claim_start_date_after_claim_end_date = 1) or
        (dq.admission_date_not_unique = 1) or
        (dq.admission_date_missing = 1) or
        (dq.discharge_date_not_unique = 1) or
        (dq.discharge_date_missing = 1) or
        (dq.admission_date_after_discharge_date = 1) or
        (dq.admit_type_code_not_unique = 1) or
        (dq.admit_type_code_missing = 1) or
        (dq.admit_source_code_not_unique = 1) or
        (dq.admit_source_code_missing = 1) or	
        (dq.claim_type_not_unique = 1) or
        (dq.claim_type_missing = 1) or
        (dq.claim_type_not_institutional = 1) ) then 1
    else 0
  end as dq_insight,

  case
    when (h.date_used_as_start_date is null) then 1
    else 0
  end as start_date_not_determined,
  
  case
    when (h.date_used_as_end_date is null) then 1
    else 0
  end as end_date_not_determined,

  case
    when (h.start_date > h.end_date) then 1
    else 0
  end as start_date_after_end_date,
  
  dq.patient_id_not_unique as patient_id_not_unique,
  dq.patient_id_missing as patient_id_missing,
  dq.claim_start_date_not_unique as claim_start_date_not_unique,
  dq.claim_start_date_missing as claim_start_date_missing,
  dq.claim_end_date_not_unique as claim_end_date_not_unique,
  dq.claim_end_date_missing as claim_end_date_missing,
  dq.claim_start_date_after_claim_end_date
       as claim_start_date_after_claim_end_date,
  dq.admission_date_not_unique as admission_date_not_unique,
  dq.admission_date_missing as admission_date_missing,
  dq.discharge_date_not_unique as discharge_date_not_unique,
  dq.discharge_date_missing as discharge_date_missing,
  dq.admission_date_after_discharge_date
       as admission_date_after_discharge_date,
  dq.admit_type_code_not_unique
       as admit_type_code_not_unique,
  dq.admit_type_code_missing
       as admit_type_code_missing,
  dq.admit_source_code_not_unique
       as admit_source_code_not_unique,
  dq.admit_source_code_missing
       as admit_source_code_missing,
  dq.discharge_disposition_code_not_unique
       as discharge_disposition_code_not_unique,
  dq.discharge_disposition_code_missing
       as discharge_disposition_code_missing,
  dq.facility_npi_not_unique as facility_npi_not_unique,
  dq.facility_npi_missing as facility_npi_missing,
  dq.claim_type_not_unique as claim_type_not_unique,
  dq.claim_type_missing as claim_type_missing,
  dq.claim_type_not_institutional as claim_type_not_institutional,
  h.data_source,
  '2024-02-18 21:13:49.400698+00:00' as tuva_last_run

from header_level_values h
left join data_quality_flags dq
  on h.claim_id = dq.claim_id
    );
  
  