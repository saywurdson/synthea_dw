WITH ObservationPeriod AS (
    SELECT 
        person_id,
        observation_id,
        observation_source_value,
        MIN(observation_datetime) AS observation_period_start_datetime,
        MAX(observation_datetime) AS observation_period_end_datetime
    FROM {{ ref('observation') }}
    GROUP BY person_id, observation_source_value, observation_id
)

SELECT 
    observation_id || '-' || LOWER(SUBSTR(HEX(RANDOM()), 1, 8)) AS observation_period_id,
    person_id,
    CAST(observation_period_start_datetime AS DATE) AS observation_period_start_date,
    CAST(observation_period_end_datetime AS DATE) AS observation_period_end_date,
    32817 AS period_type_concept_id
FROM ObservationPeriod