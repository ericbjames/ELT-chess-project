WITH my_games_staging AS (
  SELECT *,
    {{ dbt_utils.generate_surrogate_key(['ID']) }} AS game_key
  FROM {{ ref('my_games_staging') }}
),

dim_opening AS (
  SELECT *
  FROM {{ ref('dim_opening') }}
),

dim_date AS (
  SELECT *
  FROM {{ ref('dim_date') }}
),
fct_game AS (
  SELECT *
  FROM {{ ref('fct_game')}}
),
dim_result AS (
  SELECT *
  FROM {{ ref('dim_result')}}
),

merged_data AS (
  SELECT 
    my_games_staging.game_key,
    fct_game.clock_increment,
    fct_game.clock_initial,
    fct_game.clock_totaltime,
    fct_game.moves,
    fct_game.perf,
    fct_game.speed,
    fct_game.status,
    dim_result.black_rating,
    dim_result.black_ratingdiff,
    dim_result.black_user_id,
    dim_result.black_user_name,
    dim_result.white_rating,
    dim_result.white_ratingdiff,
    dim_result.white_user_id,
    dim_result.white_user_name,
    dim_result.winner,
    dim_opening.opening_name,
    dim_opening.opening_eco,
    dim_opening.first_opening_variation,
    dim_opening.opening_url,
    dim_date.date_quarter,
    dim_date.date_month,
    dim_date.time_of_day,
    dim_date.date_year,
    dim_date.date_day,
    dim_date.created_at,
    dim_date.last_move_at,
    ROW_NUMBER() OVER (PARTITION BY my_games_staging.game_key ORDER BY dim_date.created_at DESC) AS row_num,
    CASE
      WHEN dim_result.black_user_name = 'penguingim1' AND dim_result.winner = 'black' THEN 1
      WHEN dim_result.white_user_name = 'penguingim1' AND dim_result.winner = 'white' THEN 1
      ELSE 0
    END AS user_win,
    CASE
      WHEN fct_game.status = 'draw' THEN 1
      ELSE 0
    END AS user_draw,
     CASE
      WHEN dim_result.black_user_name = 'penguingim1' THEN dim_result.black_rating
      WHEN dim_result.white_user_name = 'penguingim1' THEN dim_result.white_rating
    END AS user_elo,
       CASE
      WHEN dim_result.black_user_name = 'penguingim1' THEN dim_result.black_ratingdiff
      WHEN dim_result.white_user_name = 'penguingim1' THEN dim_result.white_ratingdiff
    END AS user_elo_diff
  FROM my_games_staging
  JOIN dim_opening ON my_games_staging.opening_name = dim_opening.opening_name
  JOIN dim_date ON my_games_staging.created_at = dim_date.created_at
  JOIN dim_result ON my_games_staging.game_key = dim_result.result_key
  JOIN fct_game ON my_games_staging.game_key = fct_game.game_key
  WHERE my_games_staging.perf IN ('blitz', 'bullet', 'ultrabullet')
)

SELECT *
FROM merged_data
WHERE row_num = 1
