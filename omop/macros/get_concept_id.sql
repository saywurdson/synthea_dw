-- macros/get_concept_id.sql

{% macro get_concept_id(concept_field, value, vocabulary_id, domain_id, concept_class_id) %}
    COALESCE((
        SELECT c.concept_id
        FROM {{ source('reference', 'concept') }} c
        WHERE c.{{ concept_field }} = REPLACE(CAST(JSON_EXTRACT(pd.extension_raw, '{{ value }}') AS VARCHAR), '"', '')
        AND c.vocabulary_id = '{{ vocabulary_id }}'
        AND c.domain_id = '{{ domain_id }}'
        AND c.invalid_reason IS NULL
        AND c.standard_concept = 'S'
        AND c.concept_class_id = '{{ concept_class_id }}'
    ), 0)
{% endmacro %}