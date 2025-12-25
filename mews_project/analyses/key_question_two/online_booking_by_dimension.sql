with

	base as(
		select *
		from fct__online_checkins
	),

	age_group as (
		select
	        'age_group' as dimension_type,
	        age_group::varchar as value,
	        sum(total_reservations) as amount_of_reservations,
	        sum(total_online_checkin) as amount_of_online_checkin
	    from fct__online_checkins
	    group by age_group
		order by amount_of_online_checkin desc
		limit 1
	),

	gender as (
		select
	        'gender' as dimension_type,
	        gender::varchar as value,
	        sum(total_reservations) as amount_of_reservations,
	        sum(total_online_checkin) as amount_of_online_checkin
	    from fct__online_checkins
	    group by gender
		order by amount_of_online_checkin desc
		limit 1
	),

	nationality as (
		select
	        'nationality_code' as dimension,
	        nationality_code::varchar as value,
	        sum(total_reservations) as amount_of_reservations,
	        sum(total_online_checkin) as amount_of_online_checkin
	    from fct__online_checkins
	    group by nationality_code
		order by amount_of_online_checkin desc
		limit 1
	),

	final as (
		select *
		from age_group

		union

		select *
		from gender

		union

		select *
		from nationality
	)

select * from final