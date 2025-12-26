with 

    base as (
        select *
        from  {{ ref('int__reservations_rates') }}
    ),

    final as (
        select
            created_utc,
            -- Dimensions
            age_group,
            gender,
            nationality_code,
            rate_name,
            night_count,
            night_cost_sum,
            -- Measures for the calculation
            occupied_space_sum,
            guest_count_sum,
            -- Normalize the revenue by dividing the total cost by the capacity units used
            -- Case statements prevent divisions by zero
            case 
                when occupied_space_sum > 0 
                    then night_cost_sum / occupied_space_sum 
                else 0 
            end as rev_per_capacity

        from base
        order by rev_per_capacity desc
    )

select * from final