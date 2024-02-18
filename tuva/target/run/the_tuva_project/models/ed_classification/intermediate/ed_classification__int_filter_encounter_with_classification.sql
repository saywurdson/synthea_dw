
  
    
    

    create  table
      "synthea"."ed_classification"."_int_filter_encounter_with_classification__dbt_tmp"
  
    as (
      /*
Filter conditions to those that were classified and pick the classification
with the greatest probability (that's the greatest logic). This logic removes
any rows that were not classified.
*/


select
   a.*
   , case greatest(edcnnpa, edcnpa, epct, noner, injury, psych, alcohol, drug)
          when edcnnpa then 'edcnnpa'
          when edcnpa then 'edcnpa'
          when epct then 'epct'
          when noner then 'noner'
          when injury then 'injury'
          when psych then 'psych'
          when alcohol then 'alcohol'
          when drug then 'drug'
          else 'unclassified'
   end as classification
from "synthea"."ed_classification"."_int_by_provider_parent_organization" a
where ed_classification_capture = 1
    );
  
  