with reviews as (
    select * from {{ ref('stg_gcash_reviews') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
),

versions as (
    select * from {{ ref('dim_app_versions') }}
)

select
    r.review_id,
    d.date_id,
    v.app_version_id,
    r.score,
    r.thumbs_up,
    r.sentiment,
    r.category,
    r.content,
    r.reviewed_at,
    r.year_month,
    r.review_year,
    r.review_month,
    case when r.reply_content is not null
        then true else false
    end as has_reply,
    r.reply_content,
    r.replied_at
from reviews r
left join dates d
    on date(cast(r.reviewed_at as timestamp)) = d.full_date
left join versions v
    on r.app_version = v.app_version