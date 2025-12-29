with

    rates_nationality as (
        select distinct on (nationality_code)
            rate_name as popular_rate,
            nationality_code,
            sum(total_reservations) as popular_rate_reservations,
            sum(
                sum(total_reservations)) over(
                    partition by nationality_code) as total_reservations
        from {{ ref('fct__rate_popularity') }}
        group by
            rate_name,
            nationality_code,
            total_reservations
        order by nationality_code, sum(total_reservations) desc
    ),

    final as (
        select
            popular_rate as "Popular Rate",
            nationality_code as "Nationality",
            popular_rate_reservations as "Reservations",
            
            {{ calculate_share('popular_rate_reservations', 'sum(popular_rate_reservations) over()') }} 
                as "Percentage of Popular Rate Reservations",
            
            total_reservations as "Total Reservations",
            
            {{ calculate_share('popular_rate_reservations', 'sum(total_reservations) over()') }} 
                as "Percentage of Popular Rate per Total Reservations"
        
        from rates_nationality
        order by popular_rate_reservations desc
    )

select * from final