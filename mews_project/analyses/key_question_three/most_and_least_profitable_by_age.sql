with

	most_profitable as(
		select
			age_group,
			avg(rev_per_capacity) as avg_revenue_per_capacity
		from fct__revenue
		group by age_group
		order by avg(rev_per_capacity) desc		
		limit 1
	),

	least_profitable as(
		select
			age_group,
			avg(rev_per_capacity) as avg_revenue_per_capacity
		from fct__revenue
		group by age_group
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