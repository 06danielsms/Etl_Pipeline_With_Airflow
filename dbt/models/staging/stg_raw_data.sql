
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
        CAST(comment_count AS BIGINT) AS comment_count
    FROM read_csv_auto('data/part-00000-f48070a2-919a-49f0-9cfd-a0398f8b4662-c000.csv')
    WHERE published_at IS NOT NULL
)

SELECT *
FROM source_data
