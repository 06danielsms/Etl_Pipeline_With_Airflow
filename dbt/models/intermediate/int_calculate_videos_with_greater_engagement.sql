WITH greater_engagement AS (
    SELECT DISTINCT
        region_code,
        video_id, 
        title, 
        channel_title, 
        like_count, 
        comment_count, 
        ROUND(like_count * 1.0 / view_count, 4) AS like_ratio, 
        ROUND(comment_count * 1.0 / view_count, 4) AS comment_ratio
    FROM {{ ref('stg_raw_data') }}
    WHERE region_code = 'US'
)

SELECT *
FROM greater_engagement




-- Insight: Nos dice qué videos generan más reacciones por parte de los usuarios.



