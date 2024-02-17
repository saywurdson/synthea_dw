
  
  create view "synthea"."main"."condition__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."condition"
  );
