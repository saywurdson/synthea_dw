
  
    
    

    create  table
      "synthea"."core"."eligibility__dbt_tmp"
  
    as (
      


select * from "synthea"."core"."_stg_claims_eligibility"
    );
  
  