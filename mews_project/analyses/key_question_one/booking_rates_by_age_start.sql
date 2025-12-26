with

	rates_age_group as (
		select distinct on (age_group)
			start_year_reservation,
			start_month_reservation,
			rate_name as popular_rate,
			age_group,
			sum(total_reservations) as popular_rate_reservations,
			sum(
				sum(total_reservations)) over(
					partition by age_group) as total_reservations
		from fct__rate_popularity
		group by
			start_year_reservation,
			start_month_reservation,
			rate_name,
			age_group,
			total_reservations
		order by age_group, sum(total_reservations) desc
	),

	calculations as (
		select
			start_year_reservation,
			start_month_reservation,
			popular_rate,
			age_group,
			popular_rate_reservations,
			round(100.0 * popular_rate_reservations / sum(popular_rate_reservations) over(),
			2 ) as percentage_pr, 
			total_reservations,
			round(100.0 * popular_rate_reservations / sum(total_reservations) over(),
			2 ) as percentage_tr 
		from rates_age_group
	)

select * from calculations