with date_spine as (
    select
        date(cast(reviewed_at as timestamp)) as review_date
    from {{ ref('stg_gcash_reviews') }}
    where reviewed_at is not null
    group by 1
)

select
    format_date('%Y%m%d', review_date)              as date_id,
    review_date                                      as full_date,
    extract(year from review_date)                   as year,
    extract(month from review_date)                  as month,
    extract(day from review_date)                    as day,
    extract(quarter from review_date)                as quarter,
    format_date('%Y-%m', review_date)                as year_month,
    format_date('%B', review_date)                   as month_name,
    format_date('%Y-Q%Q', review_date)               as year_quarter,
    case extract(dayofweek from review_date)
        when 1 then 'Sunday'
        when 2 then 'Monday'
        when 3 then 'Tuesday'
        when 4 then 'Wednesday'
        when 5 then 'Thursday'
        when 6 then 'Friday'
        when 7 then 'Saturday'
    end                                              as day_of_week,
    case
        when extract(dayofweek from review_date) in (1, 7)
        then true else false
    end                                              as is_weekend
from date_spine
order by review_date