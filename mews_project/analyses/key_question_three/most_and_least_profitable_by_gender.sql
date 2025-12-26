with

	most_profitable as(
		select
			gender,
			rate_name,
			round(avg(rev_per_capacity), 3) as avg_revenue_per_capacity
		from fct__revenue
		group by 
			gender,
			rate_name
		order by avg(rev_per_capacity) desc		
		limit 1
	),

	least_profitable as(
		select
			gender,
			rate_name,
			round(avg(rev_per_capacity), 3) as avg_revenue_per_capacity
		from fct__revenue
		group by 
			gender,
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
			case
				when gender = 0
				then 'undefined'
				when gender = 1
				then 'male'
				when gender = 2
				then 'female'
			end as gender,
			rate_name,
			avg_revenue_per_capacity
		from unioning
		order by avg_revenue_per_capacity desc
	)

select * from final