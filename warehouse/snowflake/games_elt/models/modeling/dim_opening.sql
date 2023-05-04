WITH opening_cte AS (
  SELECT DISTINCT
    opening_name,
    opening_eco,
    opening_variation,
    first_opening_variation
  FROM leaderboard_schema_staging.my_games_staging
),

opening_url_cte AS (
  SELECT
    opening_name,
    opening_eco,
    first_opening_variation,
    opening_variation,
    CONCAT(
  'https://lichess.org/opening/',
  LOWER(REPLACE(REPLACE(REPLACE(opening_name, ' ', '_'), '''', ''), ',', '')),
  CASE 
    WHEN first_opening_variation IS NOT NULL 
    THEN CONCAT(LOWER(SPLIT_PART(REPLACE(REPLACE(REPLACE(opening_variation, ' ', '_'), '''', ''), ',', ''), ',', 1)))
    ELSE ''
  END
) AS opening_url

  FROM opening_cte
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['opening_name', 'opening_eco', 'first_opening_variation']) }} AS opening_key,
  opening_name,
  first_opening_variation,
  opening_eco,
  opening_url
FROM opening_url_cte
ORDER BY opening_eco, opening_name, first_opening_variation
