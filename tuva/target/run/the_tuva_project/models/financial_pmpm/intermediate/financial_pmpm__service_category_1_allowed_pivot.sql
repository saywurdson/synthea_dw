
  
    
    

    create  table
      "synthea"."financial_pmpm"."_int_service_category_1_allowed_pivot__dbt_tmp"
  
    as (
      

with service_cat_1 as (
select
  patient_id
, year_month
, payer
, plan
, service_category_1
, data_source
, sum(total_allowed) as total_allowed
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
        then total_allowed
      else 0
      end
    )
    
      
        as inpatient_allowed
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Outpatient'
        then total_allowed
      else 0
      end
    )
    
      
        as outpatient_allowed
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Office Visit'
        then total_allowed
      else 0
      end
    )
    
      
        as office_visit_allowed
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Ancillary'
        then total_allowed
      else 0
      end
    )
    
      
        as ancillary_allowed
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Other'
        then total_allowed
      else 0
      end
    )
    
      
        as other_allowed
      
    
    ,
  
    sum(
      
      case
      when service_category_1 = 'Pharmacy'
        then total_allowed
      else 0
      end
    )
    
      
        as pharmacy_allowed
      
    
    
  

, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from service_cat_1
group by 1,2,3,4,5
    );
  
  