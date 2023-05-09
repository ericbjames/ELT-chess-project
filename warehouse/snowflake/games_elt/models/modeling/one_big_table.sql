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

merged_data AS (
  SELECT 
    my_games_staging.game_key,
    my_games_staging.clock_increment,
    my_games_staging.clock_initial,
    my_games_staging.clock_totaltime,
    my_games_staging.moves,
    my_games_staging.perf,
    my_games_staging.speed,
    my_games_staging.status,
    my_games_staging.black_rating,
    my_games_staging.black_ratingdiff,
    my_games_staging.black_user_id,
    my_games_staging.black_user_name,
    my_games_staging.white_rating,
    my_games_staging.white_ratingdiff,
    my_games_staging.white_user_id,
    my_games_staging.white_user_name,
    my_games_staging.winner,
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
      WHEN my_games_staging.black_user_name = 'penguingim1' AND my_games_staging.winner = 'black' THEN 1
      WHEN my_games_staging.white_user_name = 'penguingim1' AND my_games_staging.winner = 'white' THEN 1
      ELSE 0
    END AS user_win,
    CASE
      WHEN my_games_staging.status = 'draw' THEN 1
      ELSE 0
    END AS user_draw,
     CASE
      WHEN my_games_staging.black_user_name = 'penguingim1' THEN black_rating
      WHEN my_games_staging.white_user_name = 'penguingim1' THEN white_rating
    END AS user_elo,
       CASE
      WHEN my_games_staging.black_user_name = 'penguingim1' THEN black_ratingdiff
      WHEN my_games_staging.white_user_name = 'penguingim1' THEN white_ratingdiff
    END AS user_elo_diff
  FROM my_games_staging
  JOIN dim_opening ON my_games_staging.opening_name = dim_opening.opening_name
  JOIN dim_date ON my_games_staging.created_at = dim_date.created_at
  WHERE my_games_staging.perf IN ('blitz', 'bullet', 'ultrabullet')
)

SELECT *
FROM merged_data
WHERE row_num = 1
