with

	online_gender as(
		select distinct on (created_day)
		    created_day,
		    'Gender' as dimension_type,
		    case
		        when gender = 0
		        then 'undefined'
		        when gender = 1
		        then 'male'
		        when gender = 2
		        then 'female'
			end as value,
		    sum(total_reservations) as amount_of_reservations,
		    sum(total_online_checkin) as amount_of_online_checkin
		from fct__online_checkins
		group by created_day, gender
		order by created_day, sum(total_online_checkin) desc
	),

	final_calculations as (
		select
			created_day,
			dimension_type,
			value,
			amount_of_reservations,
			amount_of_online_checkin,
			round(100.0 * amount_of_online_checkin / sum(amount_of_online_checkin) over(),
			2 ) as percentage_of_online_checkins,
			round(100.0 * amount_of_online_checkin / sum(amount_of_reservations) over(),
			2 ) as percentage_of_online_per_total_reservations
		from online_gender
	),

	final as (
		select
			created_day as "Day of Week",
			dimension_type as "Dimension Type",
			value as "Value",
			amount_of_reservations as "Total Reservations",
			amount_of_online_checkin as "Online Checkins",
			cast(percentage_of_online_checkins as varchar) || '%' as "Percentage of Online Checkins",
			cast(percentage_of_online_per_total_reservations as varchar) || '%' as "Percentage of Online Checkins per Total Reservations"
		from final_calculations
		order by amount_of_online_checkin desc
	)

select * from final