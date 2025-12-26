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
	    from base
	    group by age_group
		order by amount_of_online_checkin desc
		limit 1
	),

	gender as (
		select
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
	    from base
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
	    from base
	    group by nationality_code
		order by amount_of_online_checkin desc
		limit 1
	),

	gathered as (
		select *
		from age_group

		union

		select *
		from gender

		union

		select *
		from nationality
	),

	final_calculations as (
		select
			dimension_type,
			value,
			amount_of_reservations,
			amount_of_online_checkin,
			round(100.0 * amount_of_online_checkin / sum(amount_of_reservations) over(),
			2 ) as percentage_of_online_per_total_reservations
		from gathered
	)

select * from final_calculations