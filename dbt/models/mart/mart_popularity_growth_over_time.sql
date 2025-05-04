-- This model aggregates the daily popularity growth data from the intermediate model
-- `int_popularity_growth_over_time` and creates a mart table that contains the daily aggregates of videos uploaded, total views, likes, and comments.
-- The model is used to analyze the daily growth of video popularity over time.
-- It selects the relevant columns from the intermediate model and returns the result with the specified columns.
{{ config(
    materialized='incremental',
    unique_key=['published_at', 'date_of_ingestion'],
    on_conflict='update'
) }}

WITH daily_aggregates AS(
    SELECT 
        published_at, 
        videos_subidos, 
        total_views, 
        total_likes, 
        total_comments,
        CURRENT_DATE AS date_of_ingestion
    FROM {{ ref('int_popularity_growth_over_time') }}
    {% if is_incremental() %}
    WHERE date_of_ingestion >= (SELECT MAX(date_of_ingestion) FROM {{ this }})
    {% endif %}
    
)

SELECT *
FROM daily_aggregates
ORDER BY published_at DESC

--hitorico de videos subidos y su popularidad