with 
    online_age_group as (
        select distinct on (created_day)
            created_day,
            'Age Group' as dimension_type,
            age_group::varchar as value,
            sum(total_reservations) as amount_of_reservations,
            sum(total_online_checkin) as amount_of_online_checkin
        from {{ ref('fct__online_checkins') }}
        group by created_day, age_group
        order by created_day, sum(total_online_checkin) desc
    ),

    final as (
        select
            created_day as "Day of Week",
            dimension_type as "Dimension Type",
            value as "Value",
            amount_of_reservations as "Total Reservations",
            amount_of_online_checkin as "Online Checkins",

            {{ calculate_share('amount_of_online_checkin', 'sum(amount_of_online_checkin) over()') }} 
                as "Percentage of Online Checkins",

            {{ calculate_share('amount_of_online_checkin', 'sum(amount_of_reservations) over()') }} 
                as "Percentage of Online Checkins per Total Reservations"
        
        from online_age_group
        order by "Online Checkins" desc
    )

select * from final