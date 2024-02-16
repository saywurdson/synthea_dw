
  
    
    

    create  table
      "synthea"."core"."medication__dbt_tmp"
  
    as (
      


select * from "synthea"."core"."_stg_clinical_medication"
    );
  
  