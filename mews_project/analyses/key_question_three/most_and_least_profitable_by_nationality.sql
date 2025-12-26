with

	most_profitable as(
		select
			nationality_code,
			rate_name,
			round(avg(rev_per_capacity), 3) as avg_revenue_per_capacity
		from fct__revenue
		group by 
			nationality_code,
			rate_name
		order by avg(rev_per_capacity) desc		
		limit 1
	),

	least_profitable as(
		select
			nationality_code,
			rate_name,
			round(avg(rev_per_capacity), 3) as avg_revenue_per_capacity
		from fct__revenue
		group by 
			nationality_code,
			rate_name
		order by avg(rev_per_capacity) asc		
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
			coalesce(nationality_code, 'Unknown'),
			rate_name,
			avg_revenue_per_capacity
		from unioning
		order by avg_revenue_per_capacity desc
	)

select * from final