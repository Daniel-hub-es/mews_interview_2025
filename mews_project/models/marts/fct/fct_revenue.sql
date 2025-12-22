with 

    base as (
        select *
        from  {{ ref('reservations_rates') }}
    ),

    final as (
        select
            created_utc,
            age_group,
            gender,
            nationality_code,
            rate_name,
            night_count,
            night_cost_sum,
            occupied_space_sum,
            guest_count_sum,
            case 
                when occupied_space_sum > 0 
                    then night_cost_sum / occupied_space_sum 
                else 0 
            end as rev_per_capacity

        from base
        order by rev_per_capacity desc
    )

select * from final