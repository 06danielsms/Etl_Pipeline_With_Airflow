WITH group_data AS (
    SELECT
        region_code,
        category_id,
        video_id,
        title,
        published_at,
        like_count,
        category_name,
        ROW_NUMBER() OVER (PARTITION BY region_code ORDER BY like_count DESC) AS rank,
        CAST(ingestion_date AS DATE) AS date_of_ingestion
    FROM {{ref('int_category_id_data_join')}}
)
SELECT *
FROM group_data
WHERE rank = 1
