-- macros/get_concept_id.sql

{% macro get_concept_id(concept_field, json_column, value, vocabulary_id, domain_id, additional_domain_id=None, additional_vocabulary_id=None) %}
    (
        SELECT c.concept_id
        FROM {{ source('vocabulary', 'concept') }} c
        WHERE c.{{ concept_field }} = REPLACE(CAST(JSON_EXTRACT({{ json_column }}, '{{ value }}') AS VARCHAR), '"', '')
        AND (c.vocabulary_id = '{{ vocabulary_id }}' OR c.vocabulary_id = '{{ additional_vocabulary_id }}')
        AND (c.domain_id = '{{ domain_id }}' OR c.domain_id = '{{ additional_domain_id }}')
    )
{% endmacro %}