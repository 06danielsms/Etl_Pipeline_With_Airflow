WITH data_videos AS (
    SELECT 
        category_id,
        category_name,
        COUNT(video_id) AS total_videos, 
        SUM(view_count) AS total_views, 
        SUM(like_count) AS total_likes, 
        SUM(comment_count) AS total_comments, 
        ROUND(AVG(CASE WHEN view_count > 0 THEN like_count * 1.0 / view_count ELSE NULL END), 4) AS avg_like_ratio, 
        ROUND(AVG(CASE WHEN view_count > 0 THEN comment_count * 1.0 / view_count ELSE NULL END), 4) AS avg_comment_ratio
    FROM {{ ref('int_category_id_data_join') }}
    GROUP BY category_id, category_name
    ORDER BY total_views DESC
)
SELECT *
FROM data_videos
