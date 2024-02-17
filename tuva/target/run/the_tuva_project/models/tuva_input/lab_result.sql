
  
  create view "synthea"."main"."lab_result__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."lab_result"
  );
