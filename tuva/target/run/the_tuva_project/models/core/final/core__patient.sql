
  
    
    

    create  table
      "synthea"."core"."patient__dbt_tmp"
  
    as (
      

select * from "synthea"."core"."_stg_claims_patient"
union all
select * from "synthea"."core"."_stg_clinical_patient"


    );
  
  