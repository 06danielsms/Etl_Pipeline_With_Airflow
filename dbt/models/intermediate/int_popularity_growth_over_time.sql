WITH popularity_growth_over_time AS(
    SELECT 
        published_at,
        CAST(ingestion_date AS DATE) AS date_of_ingestion, 
        COUNT(video_id) AS videos_subidos, 
        SUM(view_count) AS total_views, 
        SUM(like_count) AS total_likes, 
        SUM(comment_count) AS total_comments,
    FROM {{ ref('int_category_id_data_join') }}
    GROUP BY published_at, ingestion_date
    
)

SELECT *
FROM popularity_growth_over_time
ORDER BY published_at DESC 