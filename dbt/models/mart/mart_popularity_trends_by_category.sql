-- This model is used to create a mart table that contains the most popular trends by category.
-- It selects the relevant columns from the intermediate model calculate_popularity_trends_by_category  
-- and returns the result with the specified columns.
-- The model is used to analyze the popularity trends of videos by category.
{{ config(
    materialized='incremental',
    unique_key=['category_id', 'reference_date'],
    on_conflict='update'
) }}

WITH data_most_popular_videos AS (
    SELECT 
        category_id,
        category_name, 
        total_videos, 
        total_views, 
        total_likes, 
        total_comments, 
        avg_like_ratio, 
        avg_comment_ratio,
        CURRENT_DATE AS reference_date
    FROM {{ref('int_calculate_popularity_trends_by_category')}}
    {% if is_incremental() %}
    WHERE reference_date >= (SELECT MAX(reference_date) FROM {{ this }})
    {% endif %}
)

SELECT *
FROM data_most_popular_videos
ORDER BY avg_like_ratio DESC, avg_comment_ratio DESC

--  Insight: Podemos ver qué categorías son más vistas y cuáles generan más interacción (likes/comentarios).