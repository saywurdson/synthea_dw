-- models/condition_occurrence.sql

WITH RECURSIVE numbers AS (
    SELECT 0 AS n
    UNION ALL
    SELECT n+1 FROM numbers WHERE n+1 < 100
),
item_data AS (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS condition_occurrence_id,
        SPLIT_PART(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"', ''), '/', -1) AS person_id,
        NULL AS condition_concept_id,
        CAST(SUBSTRING(REPLACE(JSON_EXTRACT(c, '$.billablePeriod.start'), '"', ''), 1, 10) AS DATE) AS condition_start_date,
        CAST(REPLACE(JSON_EXTRACT(c, '$.billablePeriod.start'), '"', '') AS TIMESTAMP) AS condition_start_datetime,
        CAST(SUBSTRING(REPLACE(JSON_EXTRACT(c, '$.billablePeriod.end'), '"', ''), 1, 10) AS DATE) AS condition_end_date,
        CAST(REPLACE(JSON_EXTRACT(c, '$.billablePeriod.end'), '"', '') AS TIMESTAMP) AS condition_end_datetime,
        32817 AS condition_type_concept_id,
        0 AS condition_status_concept_id,
        NULL AS stop_reason,
        NULL AS provider_id,
        SPLIT_PART(REPLACE(JSON_EXTRACT(c, '$.item[0].encounter[0].reference'), '"', ''), '/', -1) AS visit_occurrence_id,
        NULL AS visit_detail_id,
        TRIM(BOTH '"' FROM JSON_EXTRACT(c, CONCAT('$.item[', numbers.n, '].productOrService.coding[0].code'))) AS condition_source_value,
        NULL AS condition_source_concept_id,
        NULL AS condition_status_source_value
    FROM
        {{ source('json', 'Claim') }} c,
        numbers
    WHERE
        JSON_EXTRACT(c, CONCAT('$.item[', numbers.n, '].diagnosisSequence')) IS NOT NULL
),
concept_data AS (
    SELECT DISTINCT
        item_data.condition_occurrence_id,
        item_data.person_id,
        COALESCE(concept_standard.concept_id, 0) AS condition_concept_id,
        item_data.condition_start_date,
        item_data.condition_start_datetime,
        item_data.condition_end_date,
        item_data.condition_end_datetime,
        item_data.condition_type_concept_id,
        item_data.condition_status_concept_id,
        item_data.stop_reason,
        vo.provider_id,
        item_data.visit_occurrence_id,
        item_data.visit_detail_id,
        item_data.condition_source_value,
        COALESCE(concept_source.concept_id, 0) AS condition_source_concept_id,
        item_data.condition_status_source_value
    FROM
        item_data
    LEFT JOIN {{ source('reference', 'concept') }} AS concept_standard
        ON item_data.condition_source_value = concept_standard.concept_code
        AND concept_standard.vocabulary_id = 'SNOMED'
        AND concept_standard.domain_id = 'Condition'
        AND concept_standard.standard_concept = 'S'
    LEFT JOIN {{ source('reference', 'concept') }} AS concept_source
        ON item_data.condition_source_value = concept_source.concept_code
        AND concept_source.vocabulary_id = 'SNOMED'
        AND concept_source.domain_id = 'Condition'
    LEFT JOIN {{ ref('visit_occurrence') }} AS vo
        ON item_data.visit_occurrence_id = vo.visit_occurrence_id
)
SELECT * FROM concept_data
WHERE 
    condition_source_concept_id IS NOT NULL
    AND condition_source_concept_id != 0

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(a, '$.id'), '"', '') AS condition_occurrence_id,
    SPLIT_PART(REPLACE(JSON_EXTRACT(a, '$.patient.reference'), '"', ''), '/', -1) AS person_id,
    {{ get_standard_concept_id('concept_code', 'a', '$.reaction[0].manifestation[0].coding[0].code', 'SNOMED', 'Condition', 'Clinical Finding') }} AS condition_concept_id,
    CAST(SUBSTRING(REPLACE(JSON_EXTRACT(a, '$.recordedDate'), '"', ''), 1, 10) AS DATE) AS condition_start_date,
    CAST(REPLACE(JSON_EXTRACT(a, '$.recordedDate'), '"', '') AS TIMESTAMP) AS condition_start_datetime,
    NULL AS condition_end_date,
    NULL AS condition_end_datetime,
    32817 AS condition_type_concept_id,
    CASE 
        WHEN REPLACE(JSON_EXTRACT(a, '$.clinicalStatus.coding[0].code'), '"', '') = 'active' THEN 9181
        ELSE 0
    END AS condition_status_concept_id,
    NULL AS stop_reason,
    vo.provider_id AS provider_id,
    vo.visit_occurrence_id AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(a, '$.reaction[0].manifestation[0].coding[0].code'), '"', '') AS condition_source_value,
    {{ get_concept_id('concept_code', 'a', '$.reaction[0].manifestation[0].coding[0].code', 'SNOMED', 'Condition', 'Clinical Finding') }} AS condition_source_concept_id,
    REPLACE(JSON_EXTRACT(a, '$.clinicalStatus.coding[0].code'), '"', '') AS condition_status_source_value
FROM {{ source('json', 'AllergyIntolerance') }} a
LEFT JOIN (
    SELECT
        person_id,
        visit_occurrence_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON SPLIT_PART(REPLACE(JSON_EXTRACT(a, '$.patient.reference'), '"', ''), '/', -1) = vo.person_id
WHERE REPLACE(JSON_EXTRACT(a, '$.code.coding[0].code'), '"', '') <> '419199007'
  AND JSON_EXTRACT(a, '$.reaction') IS NOT NULL
  AND condition_source_concept_id IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(ct, '$.id'), '"', '') AS condition_occurrence_id,
    SPLIT_PART(REPLACE(JSON_EXTRACT(ct, '$.subject.reference'), '"', ''), '/', -1) AS person_id,
    {{ get_standard_concept_id('concept_code', 'ct', '$.reasonCode[0].coding[0].code', 'SNOMED', 'Condition', 'Clinical Finding') }} AS condition_concept_id,
    CAST(SUBSTRING(REPLACE(JSON_EXTRACT(ct, '$.period.start'), '"', ''), 1, 10) AS DATE) AS condition_start_date,
    CAST(REPLACE(JSON_EXTRACT(ct, '$.period.start'), '"', '') AS TIMESTAMP) AS condition_start_datetime,
    CAST(SUBSTRING(REPLACE(JSON_EXTRACT(ct, '$.period.end'), '"', ''), 1, 10) AS DATE) AS condition_end_date,
    CAST(REPLACE(JSON_EXTRACT(ct, '$.period.end'), '"', '') AS TIMESTAMP) AS condition_end_datetime,
    32817 AS condition_type_concept_id,
    0 AS condition_status_concept_id,
    NULL AS stop_reason,
    vo.provider_id AS provider_id,
    SPLIT_PART(REPLACE(JSON_EXTRACT(ct, '$.encounter.reference'), '"', ''), '/', -1) AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(ct, '$.reasonCode[0].coding[0].code'), '"', '') AS condition_source_value,
    {{ get_concept_id('concept_code', 'ct', '$.reasonCode[0].coding[0].code', 'SNOMED', 'Condition', 'Clinical Finding') }} AS condition_source_concept_id,
    NULL AS condition_status_source_value
FROM {{ source('json', 'CareTeam') }} ct
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON SPLIT_PART(REPLACE(JSON_EXTRACT(ct, '$.subject.reference'), '"', ''), '/', -1) = vo.person_id
WHERE
    JSON_EXTRACT(ct, '$.reasonCode') IS NOT NULL
    AND condition_source_concept_id IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS condition_occurrence_id,
    SPLIT_PART(REPLACE(JSON_EXTRACT(c, '$.subject.reference'), '"', ''), '/', -1) AS person_id,
    {{ get_standard_concept_id('concept_code', 'c', '$.code.coding[0].code', 'SNOMED', 'Condition', 'Clinical Finding') }} AS condition_concept_id,
    CAST(SUBSTRING(REPLACE(JSON_EXTRACT(c, '$.onsetDateTime'), '"', ''), 1, 10) AS DATE) AS condition_start_date,
    CAST(REPLACE(JSON_EXTRACT(c, '$.onsetDateTime'), '"', '') AS TIMESTAMP) AS condition_start_datetime,
    CAST(SUBSTRING(REPLACE(JSON_EXTRACT(c, '$.abatementDateTime'), '"', ''), 1, 10) AS DATE) AS condition_end_date,
    CAST(REPLACE(JSON_EXTRACT(c, '$.abatementDateTime'), '"', '') AS TIMESTAMP) AS condition_end_datetime,
    32817 AS condition_type_concept_id,
    CASE 
        WHEN REPLACE(JSON_EXTRACT(c, '$.clinicalStatus.coding[0].code'), '"', '') = 'resolved' THEN 37109701
        WHEN REPLACE(JSON_EXTRACT(c, '$.clinicalStatus.coding[0].code'), '"', '') = 'active' THEN 9181
        ELSE NULL
    END AS condition_status_concept_id,
    NULL AS stop_reason,
    vo.provider_id AS provider_id,
    SPLIT_PART(REPLACE(JSON_EXTRACT(c, '$.encounter.reference'), '"', ''), '/', -1) AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(c, '$.code.coding[0].code'), '"', '') AS condition_source_value,
    {{ get_concept_id('concept_code', 'c', '$.code.coding[0].code', 'SNOMED', 'Condition', 'Clinical Finding') }} AS condition_source_concept_id,
    REPLACE(JSON_EXTRACT(c, '$.clinicalStatus.coding[0].code'), '"', '') AS condition_status_source_value
FROM {{ source('json', 'Condition') }} c
LEFT JOIN (
    SELECT
        visit_occurrence_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
) vo ON SPLIT_PART(REPLACE(JSON_EXTRACT(c, '$.encounter.reference'), '"', ''), '/', -1) = vo.visit_occurrence_id
WHERE condition_source_concept_id IS NOT NULL