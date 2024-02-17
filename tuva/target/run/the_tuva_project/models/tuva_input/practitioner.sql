
  
  create view "synthea"."main"."practitioner__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."practitioner"
  );
