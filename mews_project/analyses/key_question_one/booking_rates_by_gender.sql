with

	rates_gender as (
		select distinct on (gender)
			rate_name as popular_rate,
			gender,
			sum(total_reservations) as popular_rate_reservations,
			sum(
				sum(total_reservations)) over(
					partition by gender) as total_reservations
		from fct__rate_popularity
		group by
			rate_name,
			gender,
			total_reservations
		order by gender, sum(total_reservations) desc
	),

	calculations as (
		select
			popular_rate,
			gender,
			popular_rate_reservations,
			round(100.0 * popular_rate_reservations / sum(popular_rate_reservations) over(),
			2 ) as percentage_pr, 
			total_reservations,
			round(100.0 * popular_rate_reservations / sum(total_reservations) over(),
			2 ) as percentage_tr 
		from rates_gender
	),

	final as (
		select
			popular_rate as "Popular Rate",
			gender as "Gender",
			popular_rate_reservations as "Reservations",
			cast(percentage_pr as varchar) || '%' as "Percentage of Popular Rate Reservations",
			total_reservations as "Total Reservations",
			cast(percentage_tr as varchar) || '%' as "Percentage of Popular Rate per Total Reservations"
		from calculations
	)

select * from final