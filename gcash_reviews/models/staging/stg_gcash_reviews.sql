with source as (
    select *
    from {{ source('gcash_reviews', 'processed_reviews') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by review_id
        order by reviewed_at desc
    ) = 1
),

cleaned as (
    select
        review_id,
        content,
        score,
        thumbs_up,
        app_version,
        sentiment,
        category,
        cast(reviewed_at as timestamp)                     as reviewed_at,
        cast(replied_at as timestamp)                      as replied_at,
        reply_content,
        date(cast(reviewed_at as timestamp))               as review_date,
        extract(year from cast(reviewed_at as timestamp))  as review_year,
        extract(month from cast(reviewed_at as timestamp)) as review_month,
        format_date(
            '%Y-%m',
            date(cast(reviewed_at as timestamp))
        )                                                  as year_month
    from deduplicated
    where review_id is not null
)

select * from cleaned