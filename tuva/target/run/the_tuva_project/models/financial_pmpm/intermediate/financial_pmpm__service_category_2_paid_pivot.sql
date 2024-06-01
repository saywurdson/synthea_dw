
  
    
    

    create  table
      "synthea"."financial_pmpm"."_int_service_category_2_paid_pivot__dbt_tmp"
  
    as (
      

with service_cat_2 as (
select
  patient_id
, year_month
, payer
, plan
, service_category_2
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
      when service_category_2 = 'Acute Inpatient'
        then total_paid
      else 0
      end
    )
    
      
        as acute_inpatient_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Ambulance'
        then total_paid
      else 0
      end
    )
    
      
        as ambulance_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Ambulatory Surgery'
        then total_paid
      else 0
      end
    )
    
      
        as ambulatory_surgery_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Dialysis'
        then total_paid
      else 0
      end
    )
    
      
        as dialysis_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Durable Medical Equipment'
        then total_paid
      else 0
      end
    )
    
      
        as durable_medical_equipment_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Emergency Department'
        then total_paid
      else 0
      end
    )
    
      
        as emergency_department_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Home Health'
        then total_paid
      else 0
      end
    )
    
      
        as home_health_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Hospice'
        then total_paid
      else 0
      end
    )
    
      
        as hospice_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Inpatient Psychiatric'
        then total_paid
      else 0
      end
    )
    
      
        as inpatient_psychiatric_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Inpatient Rehabilitation'
        then total_paid
      else 0
      end
    )
    
      
        as inpatient_rehabilitation_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Lab'
        then total_paid
      else 0
      end
    )
    
      
        as lab_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Office Visit'
        then total_paid
      else 0
      end
    )
    
      
        as office_visit_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Outpatient Hospital or Clinic'
        then total_paid
      else 0
      end
    )
    
      
        as outpatient_hospital_or_clinic_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Outpatient Psychiatric'
        then total_paid
      else 0
      end
    )
    
      
        as outpatient_psychiatric_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Outpatient Rehabilitation'
        then total_paid
      else 0
      end
    )
    
      
        as outpatient_rehabilitation_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Skilled Nursing'
        then total_paid
      else 0
      end
    )
    
      
        as skilled_nursing_paid
      
    
    ,
  
    sum(
      
      case
      when service_category_2 = 'Urgent Care'
        then total_paid
      else 0
      end
    )
    
      
        as urgent_care_paid
      
    
    
  

, '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from service_cat_2
group by 1,2,3,4,5
    );
  
  