
  
    
    

    create  table
      "synthea"."core"."practitioner__dbt_tmp"
  
    as (
      

select * from "synthea"."core"."_stg_claims_practitioner"
union all
select * from "synthea"."core"."_stg_clinical_practitioner"


    );
  
  