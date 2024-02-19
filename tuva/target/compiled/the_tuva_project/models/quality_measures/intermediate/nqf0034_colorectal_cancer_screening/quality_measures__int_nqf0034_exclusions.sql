

with exclusions as (
select *
From "synthea"."quality_measures"."_int_nqf0034_exclude_advanced_illness"

union all

select *
From "synthea"."quality_measures"."_int_nqf0034_exclude_colectomy_cancer"

union all

select *
From "synthea"."quality_measures"."_int_nqf0034_exclude_dementia"

union all

select *
From "synthea"."quality_measures"."_int_nqf0034_exclude_hospice_palliative"

union all

select *
From "synthea"."quality_measures"."_int_nqf0034_exclude_institutional_snp"
)

select exclusions.*
from exclusions
inner join "synthea"."quality_measures"."_int_nqf0034_denominator" p
    on exclusions.patient_id = p.patient_id