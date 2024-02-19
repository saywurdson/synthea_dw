

with  __dbt__cte__financial_pmpm__stg_medical_claim as (



SELECT
  patient_id
, claim_id
, claim_line_number
, claim_start_date
, claim_end_date
, service_category_1
, service_category_2
, paid_amount
, allowed_amount
, payer
, plan
, data_source
, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."medical_claim"
),  __dbt__cte__financial_pmpm__stg_pharmacy_claim as (



SELECT
  patient_id
, dispensing_date
, paid_amount
, allowed_amount
, payer
, plan
, data_source
, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."pharmacy_claim"
), claims_with_service_categories as (
  select
      a.patient_id
    , a.payer
    , a.plan
    , a.service_category_1
    , a.service_category_2
    , coalesce(a.claim_start_date,a.claim_end_date) as claim_date
    , a.paid_amount
    , a.allowed_amount
    , data_source
  from __dbt__cte__financial_pmpm__stg_medical_claim a
)

, medical_claims_year_month as (
  select
      patient_id
    , payer
    , plan
    , service_category_1
    , service_category_2
    , cast(date_part('year', claim_date) as TEXT ) || lpad(cast(date_part('month', claim_date) as TEXT ),2,'0') AS year_month
    , paid_amount
    , allowed_amount
    , data_source
  from claims_with_service_categories
)

, rx_claims as (
  select
      patient_id
    , payer
    , plan
    , 'Pharmacy' as service_category_1
    , cast(null as TEXT) as service_category_2
    , try_cast( dispensing_date as date )  as claim_date
    , paid_amount
    , allowed_amount
    , data_source
  from __dbt__cte__financial_pmpm__stg_pharmacy_claim
)

, rx_claims_year_month as (
  select
      patient_id
    , payer
    , plan
    , service_category_1
    , service_category_2
    , cast(date_part('year', claim_date) as TEXT ) || lpad(cast(date_part('month', claim_date) as TEXT ),2,'0') AS year_month
    , paid_amount
    , allowed_amount
    , data_source
  from rx_claims
)

, combine_medical_and_rx as (
select *
from medical_claims_year_month

union all

select *
from rx_claims_year_month
)

select
    patient_id
  , year_month
  , payer
  , plan
  , service_category_1
  , service_category_2
  , sum(paid_amount) as total_paid
  , sum(allowed_amount) as total_allowed
  , data_source
  , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
  from combine_medical_and_rx
group by 1,2,3,4,5,6,9