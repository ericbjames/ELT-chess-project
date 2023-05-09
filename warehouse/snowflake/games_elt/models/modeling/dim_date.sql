{{  config(
  materialized='table',
  unique_key='date_key',
  sort='created_at, last_move_at'
)  }}

{% set date_cols = ['created_at', 'last_move_at'] %}

WITH date_cte AS (
  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(date_cols) }} AS date_key,
    CASE
      WHEN DATE_PART('quarter', created_at) = 1 THEN 'Q1'
      WHEN DATE_PART('quarter', created_at) = 2 THEN 'Q2'
      WHEN DATE_PART('quarter', created_at) = 3 THEN 'Q3'
      WHEN DATE_PART('quarter', created_at) = 4 THEN 'Q4'
    END AS date_quarter,
    MONTHNAME(created_at) AS date_month,
    DATE_TRUNC('hour', created_at)::TIME AS time_of_day,
    TO_CHAR(YEAR(created_at)) AS date_year,
    DATE_PART('dow', created_at) AS date_day,
    created_at,
    last_move_at
  FROM {{ ref('my_games_staging') }}
)

SELECT *
FROM date_cte
