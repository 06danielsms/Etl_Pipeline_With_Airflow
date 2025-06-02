WITH greater_engagement AS (
    SELECT DISTINCT
        region_code,
        video_id, 
        title, 
        channel_title, 
        like_count, 
        comment_count, 
        ROUND(CASE WHEN like_count > 0 THEN like_count * 1.0 / view_count ELSE NULL END, 4) AS like_ratio, 
        ROUND(CASE WHEN comment_count > 0 THEN comment_count * 1.0 / view_count ELSE NULL END, 4) AS comment_ratio,
        CAST(ingestion_date AS DATE) AS date_of_ingestion
    FROM {{ ref('stg_raw_data') }}
    WHERE region_code = 'US'
)

SELECT *
FROM greater_engagement




-- Insight: Nos dice qué videos generan más reacciones por parte de los usuarios.



