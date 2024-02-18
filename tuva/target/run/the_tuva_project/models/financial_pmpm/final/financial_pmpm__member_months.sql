
  
    
    

    create  table
      "synthea"."financial_pmpm"."member_months__dbt_tmp"
  
    as (
      

with  __dbt__cte__financial_pmpm__stg_eligibility as (


select
  patient_id
, enrollment_start_date
, enrollment_end_date
, payer
, plan
, data_source
, '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."eligibility"
), month_start_and_end_dates as (
select 
  concat(cast(year as TEXT ),lpad(cast(month as TEXT),2,'0')) as year_month
, min(full_date) as month_start_date
, max(full_date) as month_end_date
from "synthea"."terminology"."calendar"
group by 1
)

select distinct
  a.patient_id
, year_month
, a.payer
, a.plan
, data_source
, '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from __dbt__cte__financial_pmpm__stg_eligibility a
inner join month_start_and_end_dates b
  on a.enrollment_start_date <= b.month_end_date
  and a.enrollment_end_date >= b.month_start_date
    );
  
  