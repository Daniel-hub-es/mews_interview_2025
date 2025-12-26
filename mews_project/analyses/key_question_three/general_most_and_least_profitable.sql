with

	most_profitable as(
		select *
		from fct__revenue
		where nationality_code is not null
        and rev_per_capacity != 0
		order by rev_per_capacity desc
		limit 1
	),

	least_profitable as(
		select *
		from fct__revenue
		where nationality_code is not null
		and rev_per_capacity != 0
		order by rev_per_capacity asc
		limit 1
	),

	unioning as (
		select *
		from most_profitable

		union

		select *
		from least_profitable
	),

	final as (
		select
			age_group,
			case
				when gender = 0
				then 'undefined'
				when gender = 1
				then 'male'
				when gender = 2
				then 'female'
			end as gender,
			nationality_code,
			rate_name,
			night_count,
			round(night_cost_sum, 3) as night_cost_sum,
			occupied_space_sum,
			guest_count_sum,
			round(rev_per_capacity, 3) as rev_per_capacity
		from unioning
		order by rev_per_capacity desc
	)

	select * from final