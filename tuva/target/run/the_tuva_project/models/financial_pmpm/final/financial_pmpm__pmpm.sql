
  
    
    

    create  table
      "synthea"."financial_pmpm"."pmpm__dbt_tmp"
  
    as (
      

SELECT 
  year_month,
  payer,
  plan,
  data_source,
  count(1) as member_months,
  SUM(total_paid) / COUNT(1) AS total_paid,
  SUM(medical_paid) / COUNT(1) AS medical_paid,
  SUM(inpatient_paid) / COUNT(1) AS inpatient_paid,
  SUM(outpatient_paid) / COUNT(1) AS outpatient_paid,
  SUM(office_visit_paid) / COUNT(1) AS office_visit_paid,
  SUM(ancillary_paid) / COUNT(1) AS ancillary_paid,
  SUM(pharmacy_paid) / COUNT(1) AS pharmacy_paid,
  SUM(other_paid) / COUNT(1) AS other_paid,
  SUM(acute_inpatient_paid) / COUNT(1) AS acute_inpatient_paid,
  SUM(ambulance_paid) / COUNT(1) AS ambulance_paid,
  SUM(ambulatory_surgery_paid) / COUNT(1) AS ambulatory_surgery_paid,
  SUM(dialysis_paid) / COUNT(1) AS dialysis_paid,
  SUM(durable_medical_equipment_paid) / COUNT(1) AS durable_medical_equipment_paid,
  SUM(emergency_department_paid) / COUNT(1) AS emergency_department_paid,
  SUM(home_health_paid) / COUNT(1) AS home_health_paid,
  SUM(hospice_paid) / COUNT(1) AS hospice_paid,
  SUM(inpatient_psychiatric_paid) / COUNT(1) AS inpatient_psychiatric_paid,
  SUM(inpatient_rehabilitation_paid) / COUNT(1) AS inpatient_rehabilitation_paid,
  SUM(lab_paid) / COUNT(1) AS lab_paid,
  SUM(office_visit_paid_2) / COUNT(1) AS office_visit_paid_2,
  SUM(outpatient_hospital_or_clinic_paid) / COUNT(1) AS outpatient_hospital_or_clinic_paid,
  SUM(outpatient_psychiatric_paid) / COUNT(1) AS outpatient_psychiatric_paid,
  SUM(outpatient_rehabilitation_paid) / COUNT(1) AS outpatient_rehabilitation_paid,
  SUM(skilled_nursing_paid) / COUNT(1) AS skilled_nursing_paid,
  SUM(urgent_care_paid) / COUNT(1) AS urgent_care_paid,
  SUM(total_allowed) / COUNT(1) AS total_allowed,
  SUM(medical_allowed) / COUNT(1) AS medical_allowed,
  SUM(inpatient_allowed) / COUNT(1) AS inpatient_allowed,
  SUM(outpatient_allowed) / COUNT(1) AS outpatient_allowed,
  SUM(office_visit_allowed) / COUNT(1) AS office_visit_allowed,
  SUM(ancillary_allowed) / COUNT(1) AS ancillary_allowed,
  SUM(pharmacy_allowed) / COUNT(1) AS pharmacy_allowed,
  SUM(other_allowed) / COUNT(1) AS other_allowed,
  SUM(acute_inpatient_allowed) / COUNT(1) AS acute_inpatient_allowed,
  SUM(ambulance_allowed) / COUNT(1) AS ambulance_allowed,
  SUM(ambulatory_surgery_allowed) / COUNT(1) AS ambulatory_surgery_allowed,
  SUM(dialysis_allowed) / COUNT(1) AS dialysis_allowed,
  SUM(durable_medical_equipment_allowed) / COUNT(1) AS durable_medical_equipment_allowed,
  SUM(emergency_department_allowed) / COUNT(1) AS emergency_department_allowed,
  SUM(home_health_allowed) / COUNT(1) AS home_health_allowed,
  SUM(hospice_allowed) / COUNT(1) AS hospice_allowed,
  SUM(inpatient_psychiatric_allowed) / COUNT(1) AS inpatient_psychiatric_allowed,
  SUM(inpatient_rehabilitation_allowed) / COUNT(1) AS inpatient_rehabilitation_allowed,
  SUM(lab_allowed) / COUNT(1) AS lab_allowed,
  SUM(office_visit_allowed_2) / COUNT(1) AS office_visit_allowed_2,
  SUM(outpatient_hospital_or_clinic_allowed) / COUNT(1) AS outpatient_hospital_or_clinic_allowed,
  SUM(outpatient_psychiatric_allowed) / COUNT(1) AS outpatient_psychiatric_allowed,
  SUM(outpatient_rehabilitation_allowed) / COUNT(1) AS outpatient_rehabilitation_allowed,
  SUM(skilled_nursing_allowed) / COUNT(1) AS skilled_nursing_allowed,
  SUM(urgent_care_allowed) / COUNT(1) AS urgent_care_allowed,
  '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
FROM "synthea"."financial_pmpm"."pmpm_prep" a
GROUP BY 1,2,3,4
    );
  
  