
  
  create view "synthea"."main"."eligibility__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."eligibility"
  );
