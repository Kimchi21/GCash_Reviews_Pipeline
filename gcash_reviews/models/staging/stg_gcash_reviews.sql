with source as (
    select *
    from {{ source('gcash_reviews', 'processed_reviews') }}
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
        cast(reviewed_at as timestamp)                    as reviewed_at,
        cast(replied_at as timestamp)                     as replied_at,
        reply_content,
        date(cast(reviewed_at as timestamp))              as review_date,
        extract(year from cast(reviewed_at as timestamp)) as review_year,
        extract(month from cast(reviewed_at as timestamp)) as review_month,
        format_date(
            '%Y-%m',
            date(cast(reviewed_at as timestamp))
        )                                                 as year_month
    from source
    where review_id is not null
)

select * from cleaned