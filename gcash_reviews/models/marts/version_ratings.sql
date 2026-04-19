with base as (
    select * from {{ ref('dim_app_versions') }}
    where total_reviews >= 10
)

select
    app_version_id,
    app_version,
    total_reviews,
    avg_rating,
    positive_count,
    negative_count,
    neutral_count,
    positive_pct,
    negative_pct,
    first_seen_date,
    last_seen_date,
    active_days
from base
order by avg_rating desc