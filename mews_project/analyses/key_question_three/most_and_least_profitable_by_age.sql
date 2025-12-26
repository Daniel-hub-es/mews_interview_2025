with

	most_profitable as(
		select
		    age_group,
		    rate_name,
		    round(avg(rev_per_capacity), 3) as avg_revenue_per_capacity
		from fct__revenue
		group by 
		    age_group, 
		    rate_name
		order by avg_revenue_per_capacity desc
		limit 1
	),

	least_profitable as(
		select
		    age_group,
		    rate_name,
		    round(avg(rev_per_capacity), 3) as avg_revenue_per_capacity
		from fct__revenue
		group by 
		    age_group, 
		    rate_name
		order by avg_revenue_per_capacity asc
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