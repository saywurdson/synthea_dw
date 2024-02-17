
  
  create view "synthea"."main"."encounter__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."encounter"
  );
