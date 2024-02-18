
  
    
    

    create  table
      "synthea"."core"."pharmacy_claim__dbt_tmp"
  
    as (
      

select * from "synthea"."core"."_stg_claims_pharmacy_claim"
    );
  
  