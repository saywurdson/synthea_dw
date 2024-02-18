
  
    
    

    create  table
      "synthea"."core"."location__dbt_tmp"
  
    as (
      

select * from "synthea"."core"."_stg_claims_location"
union all
select * from "synthea"."core"."_stg_clinical_location"


    );
  
  