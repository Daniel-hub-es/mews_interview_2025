with

	rates_nationality as (
		select distinct on (nationality_code)
			created_year_reservation,
			created_month_reservation,
			rate_name as popular_rate,
			case
				when nationality_code is NULL
				then 'Unknown'
				else nationality_code
			end as nationality_code,
			sum(total_reservations) as popular_rate_reservations,
			sum(
				sum(total_reservations)) over(
					partition by nationality_code) as total_reservations
		from fct__rate_popularity
		group by
			created_year_reservation,
			created_month_reservation,
			rate_name,
			nationality_code,
			total_reservations
		order by nationality_code, sum(total_reservations) desc
	),

	calculations as (
		select
			created_year_reservation,
			created_month_reservation,
			popular_rate,
			nationality_code,
			popular_rate_reservations,
			round(100.0 * popular_rate_reservations / sum(popular_rate_reservations) over(),
			2 ) as percentage_pr, 
			total_reservations,
			round(100.0 * popular_rate_reservations / sum(total_reservations) over(),
			2 ) as percentage_tr 
		from rates_nationality
	)

select * from calculations
order by popular_rate_reservations desc