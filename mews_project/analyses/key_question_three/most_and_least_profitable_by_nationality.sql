with

	most_profitable as(
		select
			nationality_code,
			avg(rev_per_capacity) as avg_revenue_per_capacity
		from fct__revenue
		where nationality_code is not null
		and rev_per_capacity != 0
		group by nationality_code
		order by avg(rev_per_capacity) desc		
		limit 1
	),

	least_profitable as(
		select
			nationality_code,
			avg(rev_per_capacity) as avg_revenue_per_capacity
		from fct__revenue
		where nationality_code is not null
		and rev_per_capacity != 0
		group by nationality_code
		order by avg(rev_per_capacity) asc		
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
order by avg_revenue_per_capacity desc