with

	base as (
		select *
		from {{ ref('int__reservations_rates') }}
	),

	metrics as (
		select 
			is_online_checkin,
			count(*) as total_bookings,
			sum(night_cost_sum) as total_night_cost_sum,
			avg(night_cost_sum) as avg_night_cost_sum
		from base
		group by 1
	),

	proposal_metrics as (
		select
		        max(case 
						when is_online_checkin = 1 
						then total_bookings 
						else 0 
					end) as online_bookings,
		        max(case 
						when is_online_checkin = 1 
						then avg_night_cost_sum 
						else 0 
					end) as avg_online_rev,
		        max(case 
						when is_online_checkin = 0 
						then avg_night_cost_sum 
						else 0 
					end) as avg_offline_rev,
		        sum(total_night_cost_sum) as total_revenue,
		        sum(total_bookings) as global_total_bookings
		    from metrics			
	),

	calculated_diff as (
		select
			global_total_bookings,
			online_bookings,
			avg_online_rev,
			avg_offline_rev,
			(avg_online_rev - avg_offline_rev) as avg_revenue_diff,
			total_revenue as current_total_revenue,
			online_bookings * (avg_online_rev - avg_offline_rev) as online_projected_growth_revenue,
			round(
				((online_bookings * (avg_online_rev - avg_offline_rev) / total_revenue) * 100), 2
			) as percentage_revenue_growth,
			total_revenue + (online_bookings * (avg_online_rev - avg_offline_rev)) as projected_total_revenue
		from proposal_metrics
	)
	
select * from calculated_diff