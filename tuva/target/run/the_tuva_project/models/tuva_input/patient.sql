
  
  create view "synthea"."main"."patient__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."patient"
  );
