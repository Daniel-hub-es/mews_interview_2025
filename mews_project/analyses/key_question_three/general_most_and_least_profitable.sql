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

	final as (
		select *
		from most_profitable

		union

		select *
		from least_profitable
	)

select * from final