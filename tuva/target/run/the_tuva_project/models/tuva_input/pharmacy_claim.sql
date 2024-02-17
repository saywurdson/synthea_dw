
  
  create view "synthea"."main"."pharmacy_claim__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."pharmacy_claim"
  );
