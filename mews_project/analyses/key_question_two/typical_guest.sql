with

    guest as (
        select 
            age_group,
            gender,
            nationality_code,
            sum(total_online_checkin) as amount_of_online_checkin
        from fct__online_checkins
        group by age_group, gender, nationality_code, total_online_checkin
        order by sum(total_online_checkin) desc
        limit 1
    ),

    final as (
        select
            age_group as "Age Group",
            gender as "Gender",
            nationality_code as "Nationality",
            amount_of_online_checkin as "Online Checkins"
        from guest
    )

select * from final