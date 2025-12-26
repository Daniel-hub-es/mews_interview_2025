with
  -- Intermediate model (rates + reservations)
    base as (
        select *
        from  {{ ref('int__reservations_rates') }}
    ),
  -- aux dimensional calendar table with days of the week
    calendar as (
        select *
        from {{ref('dim__calendar')}}
    ),

    final as (
        select
      -- date and weekday
            base.created_utc,
            cal.weekday as created_day,
      -- dimensions
            base.age_group,
            base.gender,
            base.nationality_code,
            base.rate_name,
            count(*) as total_reservations, -- amoubnt of reservations
            sum(is_online_checkin) as total_online_checkin --amount of reservations that are online
        from base
    -- join weekdays by date with base table
        left join calendar cal
        on cast(base.created_utc as date) = cal.dates
        group by
      -- group by clause for the aggregation
            base.created_utc,
            cal.weekday,	
            base.age_group,
            base.gender,
            base.nationality_code,
            base.rate_name
    -- order records from the greatest to the lowest amount
        order by total_online_checkin desc
    )

select * from final