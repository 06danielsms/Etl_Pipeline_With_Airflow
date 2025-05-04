-- TOP VIDEO LIKES BY region
-- This model selects the top video likes by region from the intermediate model calculate_top_videos_likes
-- and returns the result with the specified columns.
{{ config(
    materialized='incremental',
    unique_key=['region_code', 'date_of_ingestion'],
    on_conflict='update'
) }}


WITH result AS (
    SELECT 
        region_code,
        category_name,
        video_id,
        title,
        published_at,
        like_count,
        CURRENT_DATE AS date_of_ingestion
    FROM {{ref('int_calculate_top_videos_likes')}}
    {% if is_incremental() %}
    WHERE date_of_ingestion >= (SELECT MAX(date_of_ingestion) FROM {{ this }})
    {% endif %}
)

SELECT *
FROM result
ORDER BY region_code ASC

--videos mas populares agrupados por region