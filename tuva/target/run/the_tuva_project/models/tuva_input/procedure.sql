
  
  create view "synthea"."main"."procedure__dbt_tmp" as (
    select *
from "synthea"."tuva_input"."procedure"
  );
