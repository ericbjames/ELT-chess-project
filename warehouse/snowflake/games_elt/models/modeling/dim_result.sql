{{  config(
  materialized='table',
  table_name='dim_result'
)  }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['ID']) }} AS result_key,
  black_rating,
  black_ratingdiff,
  black_user_id,
  black_user_name,
  white_rating,
  white_ratingdiff,
  white_user_id,
  white_user_name,
  winner
FROM {{ref('my_games_staging')}}
