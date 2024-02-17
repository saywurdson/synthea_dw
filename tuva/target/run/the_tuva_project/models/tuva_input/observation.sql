
  
  create view "synthea"."main"."observation__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."observation"
  );
