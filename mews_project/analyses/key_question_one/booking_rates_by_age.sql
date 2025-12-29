with

    rates_age_group as (
        select distinct on (age_group)
            rate_name as popular_rate,
            age_group,
            sum(total_reservations) as popular_rate_reservations,
            sum(
                sum(total_reservations)) over(
                    partition by age_group) as total_reservations
        from {{ ref('fct__rate_popularity') }}
        group by
            rate_name,
            age_group,
            total_reservations
        order by age_group, sum(total_reservations) desc
    ),

    final as (
        select
            popular_rate as "Popular Rate",
            age_group as "Age Group",
            popular_rate_reservations as "Reservations",
			
            {{ calculate_share('popular_rate_reservations', 'sum(popular_rate_reservations) over()') }} 
                as "Percentage of Popular Rate Reservations",
            
            total_reservations as "Total Reservations",
    
            {{ calculate_share('popular_rate_reservations', 'sum(total_reservations) over()') }} 
                as "Percentage of Popular Rate per Total Reservations"
        
        from rates_age_group
    )

select * from final