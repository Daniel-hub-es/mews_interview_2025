with

	rates_gender as (
		select distinct on (gender)
			created_year_reservation,
			created_month_reservation,
			rate_name as popular_rate,
			case
				when gender = 0
				then 'undefined'
				when gender = 1
				then 'male'
				when gender = 2
				then 'female'
			end as gender,
			sum(total_reservations) as popular_rate_reservations,
			sum(
				sum(total_reservations)) over(
					partition by gender) as total_reservations
		from fct__rate_popularity
		group by
			created_year_reservation,
			created_month_reservation,
			rate_name,
			gender,
			total_reservations
		order by gender, sum(total_reservations) desc
	),

	calculations as (
		select
			created_year_reservation,
			created_month_reservation,
			popular_rate,
			gender,
			popular_rate_reservations,
			round(100.0 * popular_rate_reservations / sum(popular_rate_reservations) over(),
			2 ) as percentage_pr, 
			total_reservations,
			round(100.0 * popular_rate_reservations / sum(total_reservations) over(),
			2 ) as percentage_tr 
		from rates_gender
	)

select * from calculations