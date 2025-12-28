with

	rates_nationality as (
		select distinct on (nationality_code)
			rate_name as popular_rate,
			nationality_code,
			sum(total_reservations) as popular_rate_reservations,
			sum(
				sum(total_reservations)) over(
					partition by nationality_code) as total_reservations
		from fct__rate_popularity
		group by
			rate_name,
			nationality_code,
			total_reservations
		order by nationality_code, sum(total_reservations) desc
	),

	calculations as (
		select
			popular_rate,
			nationality_code,
			popular_rate_reservations,
			round(100.0 * popular_rate_reservations / sum(popular_rate_reservations) over(),
			2 ) as percentage_pr, 
			total_reservations,
			round(100.0 * popular_rate_reservations / sum(total_reservations) over(),
			2 ) as percentage_tr 
		from rates_nationality
	),

	final as (
		select
			popular_rate as "Popular Rate",
			nationality_code as "Nationality",
			popular_rate_reservations as "Reservations",
			cast(percentage_pr as varchar) || '%' as "Percentage of Popular Rate Reservations",
			total_reservations as "Total Reservations",
			cast(percentage_tr as varchar) || '%' as "Percentage of Popular Rate per Total Reservations"
		from calculations
		order by popular_rate_reservations desc
	)

select * from final