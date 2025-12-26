with 
    online_age_group as(
        select distinct on (created_day)
            created_day,
            'age_group' as dimension_type,
            age_group::varchar as value,
            sum(total_reservations) as amount_of_reservations,
            sum(total_online_checkin) as amount_of_online_checkin
        from fct__online_checkins
        group by created_day, age_group
    order by created_day, sum(total_online_checkin) desc
    ),

    final as (
        select
            *,
            round(100.0 * amount_of_online_checkin / sum(amount_of_reservations) over(),
            2 ) as percentage_of_online_per_total_reservations
        from online_age_group
    )

select * from final