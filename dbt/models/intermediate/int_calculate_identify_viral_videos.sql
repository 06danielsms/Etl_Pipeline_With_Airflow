

WITH identify_viral_videos AS (
SELECT
    region_code,
    video_id, 
    title, 
    channel_title, 
    view_count, 
    like_count, 
    comment_count, 
    published_at,
    row_number() OVER (PARTITION BY video_id ORDER BY ingestion_date DESC) AS rank,
    ROUND(view_count * 1.0 / (CURRENT_DATE - published_at), 2) AS views_per_day,
    CAST(ingestion_date AS DATE) AS date_of_ingestion
FROM {{ref('int_category_id_data_join')}}
WHERE (CURRENT_DATE - published_at) > 0
AND region_code = 'CO'
)

SELECT *
FROM identify_viral_videos
WHERE rank = 1

