with versions as (
    select
        app_version,
        count(*)                                        as total_reviews,
        round(avg(score), 2)                            as avg_rating,
        min(date(cast(reviewed_at as timestamp)))       as first_seen_date,
        max(date(cast(reviewed_at as timestamp)))       as last_seen_date,
        countif(sentiment = 'positive')                 as positive_count,
        countif(sentiment = 'negative')                 as negative_count,
        countif(sentiment = 'neutral')                  as neutral_count
    from {{ ref('stg_gcash_reviews') }}
    where app_version is not null
    group by app_version
)

select
    {{ dbt_utils.generate_surrogate_key(['app_version']) }} as app_version_id,
    app_version,
    total_reviews,
    avg_rating,
    first_seen_date,
    last_seen_date,
    positive_count,
    negative_count,
    neutral_count,
    round(positive_count * 100.0 / total_reviews, 2)   as positive_pct,
    round(negative_count * 100.0 / total_reviews, 2)   as negative_pct,
    date_diff(last_seen_date, first_seen_date, day)     as active_days
from versions
order by first_seen_date