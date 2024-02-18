
  
    
    

    create  table
      "synthea"."core"."procedure__dbt_tmp"
  
    as (
      

select * from "synthea"."core"."_stg_claims_procedure"
union all
select * from "synthea"."core"."_stg_clinical_procedure"


    );
  
  