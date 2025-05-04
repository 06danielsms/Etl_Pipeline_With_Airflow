-- This model identifies viral videos based on their view count, like count, and comment count.
-- It calculates the average views per day for each video and filters the results based on the region code.
-- This model is used to create a mart table that contains the videos with the highest views per day.
-- It selects the relevant columns from the intermediate model calculate_identify_viral_videos

{{ config(
    materialized='incremental',
    unique_key=['video_id', 'date_of_ingestion'],
    on_conflict='update'
) }}

WITH viral_analysis AS (
    SELECT 
        video_id, 
        title, 
        channel_title, 
        view_count, 
        like_count, 
        comment_count, 
        published_at,
        views_per_day,
        CURRENT_DATE AS date_of_ingestion
    FROM {{ ref('int_calculate_identify_viral_videos') }}
    {% if is_incremental() %}
    WHERE date_of_ingestion >= (SELECT MAX(date_of_ingestion) FROM {{ this }})
    {% endif %}
)

SELECT * FROM viral_analysis
ORDER BY views_per_day DESC

--promedio de vistas por día desde su fecha de publicación
-- para detectar videos virales en una región especifica (Colombia) .