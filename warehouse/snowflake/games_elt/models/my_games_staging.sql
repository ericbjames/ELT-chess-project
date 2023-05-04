{{ config(
  materialized='table',
  table_name='my_games_staging'
) }}

with record_hashing as (
  select 
    record_metadata, 
    hash(record_content) as hashed_record_content,
    record_content
  from CHESS_DATA_DB.RAW_DATA_SCHEMA.CUSTOM_TOPIC
),
de_duped as (
  select 
    row_number() over (partition by hashed_record_content order by record_metadata:"CreateTime" desc) as rn,
    *
  from record_hashing
)
select 
  TRIM(GET(RECORD_CONTENT, 'id'),'"') AS id,
  RECORD_CONTENT:clock:increment::INTEGER AS clock_increment,
  RECORD_CONTENT:clock:initial::INTEGER AS clock_initial,
  RECORD_CONTENT:clock:totalTime::INTEGER AS clock_totalTime,
  DATEADD('SECOND', RECORD_CONTENT:createdAt / 1000, '1970-01-01') AS created_at,
  DATEADD('SECOND', RECORD_CONTENT:lastMoveAt / 1000, '1970-01-01') AS last_move_at,
  TRIM(GET(RECORD_CONTENT, 'moves'), '"') AS moves,
  TRIM(RECORD_CONTENT:opening:eco, '"') AS opening_eco,
  SPLIT_PART(RECORD_CONTENT:opening:name, ':', 1) AS opening_name,
  CASE 
      WHEN POSITION(':' IN RECORD_CONTENT:opening:name) > 0 THEN SPLIT_PART(RECORD_CONTENT:opening:name, ':', 2)
      ELSE NULL 
  END AS opening_variation,
  SPLIT_PART(opening_variation, ',', 1) AS first_opening_variation,
  RECORD_CONTENT:opening:ply::INTEGER AS opening_ply,
  TRIM(GET(RECORD_CONTENT, 'perf'),'"') AS perf,
  GET(RECORD_CONTENT, 'pgn') AS pgn,
  RECORD_CONTENT:players:black:rating::INTEGER AS black_rating,
  RECORD_CONTENT:players:black:ratingDiff::INTEGER AS black_ratingDiff,
  TRIM(RECORD_CONTENT:players:black:user:id, '"') AS black_user_id,
  TRIM(RECORD_CONTENT:players:black:user:name, '"') AS black_user_name,
  RECORD_CONTENT:players:white:rating::INTEGER AS white_rating,
  RECORD_CONTENT:players:white:ratingDiff::INTEGER AS white_ratingDiff,
  TRIM(RECORD_CONTENT:players:white:user:id, '"') AS white_user_id,
  TRIM(RECORD_CONTENT:players:white:user:name, '"') AS white_user_name,
  GET(RECORD_CONTENT, 'rated') AS rated,
  TRIM(GET(RECORD_CONTENT, 'speed'), '"') AS speed,
  TRIM(GET(RECORD_CONTENT, 'status'), '"') AS status,
  TRIM(GET(RECORD_CONTENT, 'variant'), '"') AS variant,
  TRIM(GET(RECORD_CONTENT, 'winner'), '"') AS winner
from de_duped 
where rn = 1
