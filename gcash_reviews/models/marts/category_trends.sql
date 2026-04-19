with base as (
    select * from {{ ref('fct_reviews') }}
)

select
    year_month,
    review_year,
    review_month,
    category,
    sentiment,
    count(*)                                    as review_count,
    round(avg(score), 2)                        as avg_score,
    sum(thumbs_up)                              as total_thumbs_up,
    round(
        count(*) * 100.0 / sum(count(*)) over (
            partition by year_month
        ), 2
    )                                           as category_pct
from base
group by year_month, review_year, review_month, category, sentiment
order by year_month, category, sentiment