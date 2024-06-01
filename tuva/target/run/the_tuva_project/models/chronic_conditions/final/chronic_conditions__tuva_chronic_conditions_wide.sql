
  
    
    

    create  table
      "synthea"."chronic_conditions"."tuva_chronic_conditions_wide__dbt_tmp"
  
    as (
      

with  __dbt__cte__tuva_chronic_conditions__stg_core__patient as (


select 
    patient_id
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."patient"
), condition_columns as (

    select distinct
          condition
        , condition_column_name
    from "synthea"."chronic_conditions"."_value_set_tuva_chronic_conditions_hierarchy"

)

select
      p.patient_id
    , 
  
    max(
      
      case
      when cc.condition_column_name = 'ACUTE_MYOCARDIAL_INFARCTION'
        then 1
      else 0
      end
    )
    
      
        as acute_myocardial_infarction
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'ALCOHOL'
        then 1
      else 0
      end
    )
    
      
        as alcohol
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'ALZHEIMERS_DISEASE'
        then 1
      else 0
      end
    )
    
      
        as alzheimers_disease
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'AMYOTROPHIC_LATERAL_SCLEROSIS'
        then 1
      else 0
      end
    )
    
      
        as amyotrophic_lateral_sclerosis
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'ANXIETY'
        then 1
      else 0
      end
    )
    
      
        as anxiety
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'ASTHMA'
        then 1
      else 0
      end
    )
    
      
        as asthma
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'ATHEROSCLEROSIS'
        then 1
      else 0
      end
    )
    
      
        as atherosclerosis
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'ATRIAL_FIBRILLATION'
        then 1
      else 0
      end
    )
    
      
        as atrial_fibrillation
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'ATTENTION_DEFICIT_HYPERACTIVITY_DISORDER'
        then 1
      else 0
      end
    )
    
      
        as attention_deficit_hyperactivity_disorder
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'BIPOLAR'
        then 1
      else 0
      end
    )
    
      
        as bipolar
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'BREAST_CANCER'
        then 1
      else 0
      end
    )
    
      
        as breast_cancer
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'CHRONIC_KIDNEY_DISEASE'
        then 1
      else 0
      end
    )
    
      
        as chronic_kidney_disease
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'CHRONIC_OBSTRUCTIVE_PULMONARY_DISEASE'
        then 1
      else 0
      end
    )
    
      
        as chronic_obstructive_pulmonary_disease
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'COCAINE'
        then 1
      else 0
      end
    )
    
      
        as cocaine
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'COLORECTAL_CANCER'
        then 1
      else 0
      end
    )
    
      
        as colorectal_cancer
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'CROHNS_DISEASE'
        then 1
      else 0
      end
    )
    
      
        as crohns_disease
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'CYSTIC_FIBROSIS'
        then 1
      else 0
      end
    )
    
      
        as cystic_fibrosis
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'DEMENTIA'
        then 1
      else 0
      end
    )
    
      
        as dementia
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'DEPRESSION'
        then 1
      else 0
      end
    )
    
      
        as depression
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'HEART_FAILURE'
        then 1
      else 0
      end
    )
    
      
        as heart_failure
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'HYPERLIPIDEMIA'
        then 1
      else 0
      end
    )
    
      
        as hyperlipidemia
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'HYPERTENSION'
        then 1
      else 0
      end
    )
    
      
        as hypertension
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'LUNG_CANCER'
        then 1
      else 0
      end
    )
    
      
        as lung_cancer
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'LUPUS'
        then 1
      else 0
      end
    )
    
      
        as lupus
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'METABOLIC_SYNDROME'
        then 1
      else 0
      end
    )
    
      
        as metabolic_syndrome
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'MULTIPLE_SCLEROSIS'
        then 1
      else 0
      end
    )
    
      
        as multiple_sclerosis
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'MUSCULAR_DYSTROPHY'
        then 1
      else 0
      end
    )
    
      
        as muscular_dystrophy
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'OBESITY'
        then 1
      else 0
      end
    )
    
      
        as obesity
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'OBSESSIVE_COMPULSIVE_DISORDER'
        then 1
      else 0
      end
    )
    
      
        as obsessive_compulsive_disorder
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'OPIOID'
        then 1
      else 0
      end
    )
    
      
        as opioid
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'PARKINSONS_DISEASE'
        then 1
      else 0
      end
    )
    
      
        as parkinsons_disease
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'PERSONALITY_DISORDER'
        then 1
      else 0
      end
    )
    
      
        as personality_disorder
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'POST_TRAUMATIC_STRESS_DISORDER'
        then 1
      else 0
      end
    )
    
      
        as post_traumatic_stress_disorder
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'RHEUMATOID_ARTHRITIS'
        then 1
      else 0
      end
    )
    
      
        as rheumatoid_arthritis
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'SCHIZOPHRENIA'
        then 1
      else 0
      end
    )
    
      
        as schizophrenia
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'STROKE'
        then 1
      else 0
      end
    )
    
      
        as stroke
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'TOBACCO'
        then 1
      else 0
      end
    )
    
      
        as tobacco
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'TYPE_1_DIABETES'
        then 1
      else 0
      end
    )
    
      
        as type_1_diabetes
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'TYPE_2_DIABETES'
        then 1
      else 0
      end
    )
    
      
        as type_2_diabetes
      
    
    ,
  
    max(
      
      case
      when cc.condition_column_name = 'ULCERATIVE_COLITIS'
        then 1
      else 0
      end
    )
    
      
        as ulcerative_colitis
      
    
    
  

      , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from __dbt__cte__tuva_chronic_conditions__stg_core__patient p
     left join "synthea"."chronic_conditions"."tuva_chronic_conditions_long" l
        on p.patient_id = l.patient_id
     left join condition_columns cc
        on l.condition = cc.condition
group by
    p.patient_id
    );
  
  