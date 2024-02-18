
  
    
    

    create  table
      "synthea"."core"."encounter__dbt_tmp"
  
    as (
      

select * from "synthea"."core"."_stg_claims_encounter"
union all
select * from "synthea"."core"."_stg_clinical_encounter"


    );
  
  