{{  config(
  materialized='table',
  table_name='fct_game'
)  }}

{% set date_cols = ['created_at', 'last_move_at'] %}

WITH opening_key AS (
  SELECT
    DISTINCT
    opening_name,
    opening_eco,
    first_opening_variation,
    {{ dbt_utils.generate_surrogate_key(['opening_name', 'opening_eco', 'first_opening_variation']) }} AS opening_key
  FROM {{ ref('my_games_staging') }}
)

SELECT DISTINCT
  {{ dbt_utils.generate_surrogate_key(['ID']) }} AS game_key,
  {{ dbt_utils.generate_surrogate_key(['ID']) }} AS result_key,
  {{ dbt_utils.generate_surrogate_key(date_cols) }} AS date_key,
  opening_key.opening_key,
  clock_increment,
  clock_initial,
  clock_totaltime,
  moves,
  perf,
  SUBSTRING(SPLIT_PART(my_games_staging.pgn, '\n', 2), POSITION('https://lichess.org/' IN SPLIT_PART(my_games_staging.pgn, '\n', 2)), 28) AS url,
  speed,
  status
 
FROM {{ ref('my_games_staging') }}
LEFT JOIN opening_key ON (
  my_games_staging.opening_name = opening_key.opening_name AND
  my_games_staging.opening_eco = opening_key.opening_eco AND
  my_games_staging.first_opening_variation = opening_key.first_opening_variation
)
