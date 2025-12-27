with

	rates_age_group as (
		select distinct on (age_group)
			rate_name as popular_rate,
			age_group,
			sum(total_reservations) as popular_rate_reservations,
			sum(
				sum(total_reservations)) over(
					partition by age_group) as total_reservations
		from fct__rate_popularity
		group by
			rate_name,
			age_group,
			total_reservations
		order by age_group, sum(total_reservations) desc
	),

	calculations as (
		select
			popular_rate,
			age_group,
			popular_rate_reservations,
			round(100.0 * popular_rate_reservations / sum(popular_rate_reservations) over(),
			2 ) as percentage_pr, 
			total_reservations,
			round(100.0 * popular_rate_reservations / sum(total_reservations) over(),
			2 ) as percentage_tr 
		from rates_age_group
	),

	final as (
		select
			popular_rate as "Popular Rate",
			age_group as "Age Group",
			popular_rate_reservations as "Reservations",
			cast(percentage_pr as varchar) || '%' as "Percentage of Popular Rate Reservations",
			total_reservations as "Total Reservations",
			cast(percentage_tr as varchar) || '%' as "Percentage of popular Rate per Total Reservations"
		from calculations
	)

select * from final