
  
    
    

    create  table
      "synthea"."core"."observation__dbt_tmp"
  
    as (
      


select * from "synthea"."core"."_stg_clinical_observation"
    );
  
  