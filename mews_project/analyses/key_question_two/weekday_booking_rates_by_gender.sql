with

	online_gender as(
		select distinct on (created_day)
		    created_day,
		    'gender' as dimension_type,
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

	final as (
		select
			*,
			round(100.0 * amount_of_online_checkin / sum(amount_of_reservations) over(),
			2 ) as percentage_of_online_per_total_reservations
		from online_gender
	)

select * from final