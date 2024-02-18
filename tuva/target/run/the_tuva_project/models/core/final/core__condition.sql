
  
    
    

    create  table
      "synthea"."core"."condition__dbt_tmp"
  
    as (
      


select * from "synthea"."core"."_stg_claims_condition"
union all
select * from "synthea"."core"."_stg_clinical_condition"


    );
  
  