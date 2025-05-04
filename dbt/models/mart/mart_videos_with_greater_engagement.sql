-- This model selects the top 10 videos in US with the highest engagement ratios (like and comment ratios) from the `int_calculate_videos_with_greater_engagement` model.
-- It orders the results by like ratio and comment ratio in descending order and limits the output to 10 records.   
-- The model is used to analyze the videos that generate the most engagement from users.
-- This model is used to create a mart table that contains the top 10 videos with the highest engagement ratios (like and comment ratios).
WITH greater_engagement AS (
    SELECT
        region_code,
        video_id, 
        title, 
        channel_title, 
        like_count, 
        comment_count, 
        like_ratio, 
        comment_ratio
    FROM {{ ref('int_calculate_videos_with_greater_engagement') }}
)

SELECT *
FROM greater_engagement
ORDER BY like_ratio DESC, comment_ratio DESC
LIMIT 10
-- Identifica los videos con mejor interacción en relación con su número de vistas.