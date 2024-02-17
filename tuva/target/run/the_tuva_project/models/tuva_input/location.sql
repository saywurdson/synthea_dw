
  
  create view "synthea"."main"."location__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."location"
  );
