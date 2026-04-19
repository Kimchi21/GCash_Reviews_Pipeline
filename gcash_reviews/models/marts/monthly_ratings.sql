with base as (
    select * from {{ ref('fct_reviews') }}
)

select
    year_month,
    review_year,
    review_month,
    count(*)                                    as total_reviews,
    round(avg(score), 2)                        as avg_rating,
    countif(score = 5)                          as five_star,
    countif(score = 4)                          as four_star,
    countif(score = 3)                          as three_star,
    countif(score = 2)                          as two_star,
    countif(score = 1)                          as one_star,
    countif(sentiment = 'positive')             as positive_count,
    countif(sentiment = 'negative')             as negative_count,
    countif(sentiment = 'neutral')              as neutral_count,
    countif(has_reply = true)                   as replied_count,
    round(
        countif(has_reply = true) * 100.0 / count(*),
        2
    )                                           as reply_rate_pct
from base
group by year_month, review_year, review_month
order by year_month