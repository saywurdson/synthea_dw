
{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}


select *
from {{ source('tuva_input', 'medical_claim') }}
