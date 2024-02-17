
  
  create view "synthea"."main"."medical_claim__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."medical_claim"
  );
