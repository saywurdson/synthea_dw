-- macros/get_standard_concept_id.sql

{% macro get_standard_concept_id(concept_field, json_column, value, vocabulary_id, domain_id, concept_class_id, additional_domain_id=None) %}
    COALESCE((
        SELECT c.concept_id
        FROM {{ source('reference', 'concept') }} c
        WHERE c.{{ concept_field }} = REPLACE(CAST(JSON_EXTRACT({{ json_column }}, '{{ value }}') AS VARCHAR), '"', '')
        AND c.vocabulary_id = '{{ vocabulary_id }}'
        AND (c.domain_id = '{{ domain_id }}' OR c.domain_id = '{{ additional_domain_id }}')
        AND c.invalid_reason IS NULL
        AND c.standard_concept = 'S'
        AND c.concept_class_id = '{{ concept_class_id }}'
    ), 0)
{% endmacro %}