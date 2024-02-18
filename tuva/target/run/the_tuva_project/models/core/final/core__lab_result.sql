
  
    
    

    create  table
      "synthea"."core"."lab_result__dbt_tmp"
  
    as (
      

select * from "synthea"."core"."_stg_clinical_lab_result"
    );
  
  