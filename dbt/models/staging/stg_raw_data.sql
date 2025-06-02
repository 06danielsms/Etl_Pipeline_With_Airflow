
with source_data as (
    SELECT
        region_code,
        category_id,
        video_id,
        title,
        channel_title,
        CAST(published_at AS DATE) AS published_at,
        CAST(view_count AS BIGINT) AS view_count,
        CAST(like_count AS BIGINT) AS like_count,
        CAST(comment_count AS BIGINT) AS comment_count,
        ingestion_date
    FROM read_csv_auto('/data/youtube_data.csv')
    WHERE published_at IS NOT NULL
    AND ingestion_date = (SELECT MAX(ingestion_date) FROM read_csv_auto('data/youtube_data.csv'))
)
SELECT *
FROM source_data
