WITH join_data AS (
    SELECT 
        rw.region_code,
        rw.category_id,
        ct.category_name,
        rw.video_id,
        rw.title,
        rw.channel_title,
        rw.published_at,
        rw.view_count,
        rw.like_count,
        rw.comment_count,
        ingestion_date
    FROM {{ ref('stg_raw_data') }} rw
    JOIN {{ ref('stg_category_id') }} ct
    ON rw.category_id = ct.id_category
)

SELECT *
FROM join_data