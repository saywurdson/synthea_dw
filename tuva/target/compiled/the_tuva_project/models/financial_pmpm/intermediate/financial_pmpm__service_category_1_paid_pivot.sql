

with service_cat_1 as (
select
  patient_id
, year_month
, payer
, plan
, service_category_1
, data_source
, sum(total_paid) as total_paid
from "synthea"."financial_pmpm"."_int_patient_spend_with_service_categories"
group by 1,2,3,4,5,6
)

select
  patient_id
, year_month
, payer
, plan
, data_source
, 
  
    sum(
      
      case
      when service_category_1 = 'Inpatient'
        then total_paid
      else 0
      end
    )
    
      
        as inpatient_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Outpatient'
        then total_paid
      else 0
      end
    )
    
      
        as outpatient_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Office Visit'
        then total_paid
      else 0
      end
    )
    
      
        as office_visit_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Ancillary'
        then total_paid
      else 0
      end
    )
    
      
        as ancillary_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Other'
        then total_paid
      else 0
      end
    )
    
      
        as other_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Pharmacy'
        then total_paid
      else 0
      end
    )
    
      
        as pharmacy_paid
      
    
    
  

, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from service_cat_1
group by 1,2,3,4,5