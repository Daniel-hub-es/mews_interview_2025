with

    rates_gender as (
        select distinct on (gender)
            rate_name as popular_rate,
            gender,
            sum(total_reservations) as popular_rate_reservations,
            sum(
                sum(total_reservations)) over(
                    partition by gender) as total_reservations
        from {{ ref('fct__rate_popularity') }}
        group by
            rate_name,
            gender,
            total_reservations
        order by gender, sum(total_reservations) desc
    ),

    final as (
        select
            popular_rate as "Popular Rate",
            gender as "Gender",
            popular_rate_reservations as "Reservations",
            
            {{ calculate_share('popular_rate_reservations', 'sum(popular_rate_reservations) over()') }} 
                as "Percentage of Popular Rate Reservations",
            
            total_reservations as "Total Reservations",
            
            {{ calculate_share('popular_rate_reservations', 'sum(total_reservations) over()') }} 
                as "Percentage of Popular Rate per Total Reservations"
        
        from rates_gender
    )

select * from final