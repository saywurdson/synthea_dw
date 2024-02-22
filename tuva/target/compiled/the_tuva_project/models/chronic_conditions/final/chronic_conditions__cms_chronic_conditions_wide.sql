

with  __dbt__cte__cms_chronic_conditions__stg_core__patient as (


select 
    patient_id
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."patient"
), chronic_conditions as (

    select distinct
          condition
        , condition_column_name
    from "synthea"."chronic_conditions"."_value_set_cms_chronic_conditions_hierarchy"

)

, conditions as (

    select
          chronic_conditions_unioned.patient_id
        , chronic_conditions.condition_column_name
        , 1 as condition_count
    from "synthea"."chronic_conditions"."cms_chronic_conditions_long" as chronic_conditions_unioned
         inner join chronic_conditions as chronic_conditions
             on chronic_conditions_unioned.condition = chronic_conditions.condition

)

select
      p.patient_id
    , 
  
    max(
      
      case
      when condition_column_name = 'acute_myocardial_infarction'
        then 1
      else 0
      end
    )
    
      
        as acute_myocardial_infarction
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'adhd_conduct_disorders_and_hyperkinetic_syndrome'
        then 1
      else 0
      end
    )
    
      
        as adhd_conduct_disorders_and_hyperkinetic_syndrome
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'alcohol_use_disorders'
        then 1
      else 0
      end
    )
    
      
        as alcohol_use_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'alzheimers_disease'
        then 1
      else 0
      end
    )
    
      
        as alzheimers_disease
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'anemia'
        then 1
      else 0
      end
    )
    
      
        as anemia
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'anxiety_disorders'
        then 1
      else 0
      end
    )
    
      
        as anxiety_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'asthma'
        then 1
      else 0
      end
    )
    
      
        as asthma
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'atrial_fibrillation_and_flutter'
        then 1
      else 0
      end
    )
    
      
        as atrial_fibrillation_and_flutter
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'autism_spectrum_disorders'
        then 1
      else 0
      end
    )
    
      
        as autism_spectrum_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'benign_prostatic_hyperplasia'
        then 1
      else 0
      end
    )
    
      
        as benign_prostatic_hyperplasia
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'bipolar_disorder'
        then 1
      else 0
      end
    )
    
      
        as bipolar_disorder
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cancer_breast'
        then 1
      else 0
      end
    )
    
      
        as cancer_breast
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cancer_colorectal'
        then 1
      else 0
      end
    )
    
      
        as cancer_colorectal
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cancer_endometrial'
        then 1
      else 0
      end
    )
    
      
        as cancer_endometrial
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cancer_lung'
        then 1
      else 0
      end
    )
    
      
        as cancer_lung
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cancer_prostate'
        then 1
      else 0
      end
    )
    
      
        as cancer_prostate
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cancer_urologic_kidney_renal_pelvis_and_ureter'
        then 1
      else 0
      end
    )
    
      
        as cancer_urologic_kidney_renal_pelvis_and_ureter
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cataract'
        then 1
      else 0
      end
    )
    
      
        as cataract
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cerebral_palsy'
        then 1
      else 0
      end
    )
    
      
        as cerebral_palsy
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'chronic_kidney_disease'
        then 1
      else 0
      end
    )
    
      
        as chronic_kidney_disease
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'chronic_obstructive_pulmonary_disease'
        then 1
      else 0
      end
    )
    
      
        as chronic_obstructive_pulmonary_disease
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'cystic_fibrosis_and_other_metabolic_developmental_disorders'
        then 1
      else 0
      end
    )
    
      
        as cystic_fibrosis_and_other_metabolic_developmental_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'depression_bipolar_or_other_depressive_mood_disorders'
        then 1
      else 0
      end
    )
    
      
        as depression_bipolar_or_other_depressive_mood_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'depressive_disorders'
        then 1
      else 0
      end
    )
    
      
        as depressive_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'diabetes'
        then 1
      else 0
      end
    )
    
      
        as diabetes
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'drug_use_disorders'
        then 1
      else 0
      end
    )
    
      
        as drug_use_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'epilepsy'
        then 1
      else 0
      end
    )
    
      
        as epilepsy
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'fibromyalgia_and_chronic_pain_and_fatigue'
        then 1
      else 0
      end
    )
    
      
        as fibromyalgia_and_chronic_pain_and_fatigue
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'glaucoma'
        then 1
      else 0
      end
    )
    
      
        as glaucoma
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'heart_failure_and_non_ischemic_heart_disease'
        then 1
      else 0
      end
    )
    
      
        as heart_failure_and_non_ischemic_heart_disease
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hepatitis_a'
        then 1
      else 0
      end
    )
    
      
        as hepatitis_a
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hepatitis_b_acute_or_unspecified'
        then 1
      else 0
      end
    )
    
      
        as hepatitis_b_acute_or_unspecified
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hepatitis_b_chronic'
        then 1
      else 0
      end
    )
    
      
        as hepatitis_b_chronic
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hepatitis_c_acute'
        then 1
      else 0
      end
    )
    
      
        as hepatitis_c_acute
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hepatitis_c_chronic'
        then 1
      else 0
      end
    )
    
      
        as hepatitis_c_chronic
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hepatitis_c_unspecified'
        then 1
      else 0
      end
    )
    
      
        as hepatitis_c_unspecified
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hepatitis_d'
        then 1
      else 0
      end
    )
    
      
        as hepatitis_d
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hepatitis_e'
        then 1
      else 0
      end
    )
    
      
        as hepatitis_e
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hip_pelvic_fracture'
        then 1
      else 0
      end
    )
    
      
        as hip_pelvic_fracture
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids'
        then 1
      else 0
      end
    )
    
      
        as human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hyperlipidemia'
        then 1
      else 0
      end
    )
    
      
        as hyperlipidemia
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hypertension'
        then 1
      else 0
      end
    )
    
      
        as hypertension
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'hypothyroidism'
        then 1
      else 0
      end
    )
    
      
        as hypothyroidism
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'intellectual_disabilities_and_related_conditions'
        then 1
      else 0
      end
    )
    
      
        as intellectual_disabilities_and_related_conditions
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'ischemic_heart_disease'
        then 1
      else 0
      end
    )
    
      
        as ischemic_heart_disease
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'learning_disabilities'
        then 1
      else 0
      end
    )
    
      
        as learning_disabilities
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'leukemias_and_lymphomas'
        then 1
      else 0
      end
    )
    
      
        as leukemias_and_lymphomas
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis'
        then 1
      else 0
      end
    )
    
      
        as liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'migraine_and_chronic_headache'
        then 1
      else 0
      end
    )
    
      
        as migraine_and_chronic_headache
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'mobility_impairments'
        then 1
      else 0
      end
    )
    
      
        as mobility_impairments
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'multiple_sclerosis_and_transverse_myelitis'
        then 1
      else 0
      end
    )
    
      
        as multiple_sclerosis_and_transverse_myelitis
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'muscular_dystrophy'
        then 1
      else 0
      end
    )
    
      
        as muscular_dystrophy
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'non_alzheimers_dementia'
        then 1
      else 0
      end
    )
    
      
        as non_alzheimers_dementia
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'obesity'
        then 1
      else 0
      end
    )
    
      
        as obesity
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'opioid_use_disorder_oud'
        then 1
      else 0
      end
    )
    
      
        as opioid_use_disorder_oud
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'osteoporosis_with_or_without_pathological_fracture'
        then 1
      else 0
      end
    )
    
      
        as osteoporosis_with_or_without_pathological_fracture
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'other_developmental_delays'
        then 1
      else 0
      end
    )
    
      
        as other_developmental_delays
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'parkinsons_disease_and_secondary_parkinsonism'
        then 1
      else 0
      end
    )
    
      
        as parkinsons_disease_and_secondary_parkinsonism
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'peripheral_vascular_disease_pvd'
        then 1
      else 0
      end
    )
    
      
        as peripheral_vascular_disease_pvd
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'personality_disorders'
        then 1
      else 0
      end
    )
    
      
        as personality_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'pneumonia_all_cause'
        then 1
      else 0
      end
    )
    
      
        as pneumonia_all_cause
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'post_traumatic_stress_disorder_ptsd'
        then 1
      else 0
      end
    )
    
      
        as post_traumatic_stress_disorder_ptsd
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'pressure_and_chronic_ulcers'
        then 1
      else 0
      end
    )
    
      
        as pressure_and_chronic_ulcers
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'rheumatoid_arthritis_osteoarthritis'
        then 1
      else 0
      end
    )
    
      
        as rheumatoid_arthritis_osteoarthritis
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'schizophrenia'
        then 1
      else 0
      end
    )
    
      
        as schizophrenia
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'schizophrenia_and_other_psychotic_disorders'
        then 1
      else 0
      end
    )
    
      
        as schizophrenia_and_other_psychotic_disorders
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'sensory_blindness_and_visual_impairment'
        then 1
      else 0
      end
    )
    
      
        as sensory_blindness_and_visual_impairment
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'sensory_deafness_and_hearing_impairment'
        then 1
      else 0
      end
    )
    
      
        as sensory_deafness_and_hearing_impairment
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'sickle_cell_disease'
        then 1
      else 0
      end
    )
    
      
        as sickle_cell_disease
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'spina_bifida_and_other_congenital_anomalies_of_the_nervous_system'
        then 1
      else 0
      end
    )
    
      
        as spina_bifida_and_other_congenital_anomalies_of_the_nervous_system
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'spinal_cord_injury'
        then 1
      else 0
      end
    )
    
      
        as spinal_cord_injury
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'stroke_transient_ischemic_attack'
        then 1
      else 0
      end
    )
    
      
        as stroke_transient_ischemic_attack
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'tobacco_use'
        then 1
      else 0
      end
    )
    
      
        as tobacco_use
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage'
        then 1
      else 0
      end
    )
    
      
        as traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage
      
    
    ,
  
    max(
      
      case
      when condition_column_name = 'viral_hepatitis_general'
        then 1
      else 0
      end
    )
    
      
        as viral_hepatitis_general
      
    
    
  

      , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from __dbt__cte__cms_chronic_conditions__stg_core__patient p
     left join conditions
        on p.patient_id = conditions.patient_id
group by
    p.patient_id