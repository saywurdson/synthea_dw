
  
  create view "synthea"."main"."medication__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."medication"
  );
