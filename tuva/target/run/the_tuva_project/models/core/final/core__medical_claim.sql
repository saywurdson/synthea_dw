
  
    
    

    create  table
      "synthea"."core"."medical_claim__dbt_tmp"
  
    as (
      


select * from "synthea"."core"."_stg_claims_medical_claim"
    );
  
  