WITH popularity_growth_over_time AS(
    SELECT 
        published_at, 
        COUNT(video_id) AS videos_subidos, 
        SUM(view_count) AS total_views, 
        SUM(like_count) AS total_likes, 
        SUM(comment_count) AS total_comments,
    FROM {{ ref('int_category_id_data_join') }}
    GROUP BY published_at
    
)

SELECT *
FROM popularity_growth_over_time
ORDER BY published_at DESC 