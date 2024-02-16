
  
    
    

    create  table
      "synthea"."claims_expanded"."medical_claim_expanded__dbt_tmp"
  
    as (
      


select *
from "synthea"."tuva_input"."medical_claim"
    );
  
  