with

    online_nationality as(
        select distinct on (created_day)
            created_day,
            'nationality_code' as dimension_type,
            nationality_code as value,
            sum(total_reservations) as amount_of_reservations,
            sum(total_online_checkin) as amount_of_online_checkin
        from fct__online_checkins
        group by created_day, nationality_code
        order by created_day, sum(total_online_checkin) desc
    ),  

    final as (
        select
            *,
            round(100.0 * amount_of_online_checkin / sum(amount_of_reservations) over(),
            2 ) as percentage_of_online_per_total_reservations
        from online_nationality
    )   

select * from final