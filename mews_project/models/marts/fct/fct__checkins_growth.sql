with
    base as (
        select *
        from {{ ref('int__reservations_rates') }}
    ),

    metrics as (
        select 
            is_online_checkin,
            count(*) as total_bookings,
			-- Total revenue related metrics
            sum(night_cost_sum) as total_night_cost_sum,
            sum(night_count) as total_nights,
            avg(night_cost_sum) as avg_stay_value,
            -- ADR: Average Daily Rate
            sum(night_cost_sum) / nullif(sum(night_count), 0) as adr,
            avg(night_count) as avg_los
        from base
        group by 1
    ),

    -- Pivot data to align Online and Offline metrics in one row (conditional aggregation)
    proposal_metrics as (
        select
            max(case
					when is_online_checkin = 1
					then total_bookings
					else 0
				end) as online_bookings,
            max(case
					when is_online_checkin = 1
					then avg_stay_value 
					else 0 
				end) as avg_online_rev,
            max(case 
					when is_online_checkin = 1 
					then adr 
					else 0 
				end) as online_adr,
            max(case 
					when is_online_checkin = 1 
					then avg_los 
					else 0 
				end) as online_los,
            
            max(case 
					when is_online_checkin = 0 
					then avg_stay_value 
					else 0 
				end) as avg_offline_rev,
            max(case 
					when is_online_checkin = 0 
					then adr 
					else 0 
				end) as offline_adr,
            max(case 
					when is_online_checkin = 0 
					then avg_los 
					else 0 
				end) as offline_los,
            sum(total_night_cost_sum) as current_total_revenue,
            sum(total_bookings) as global_total_bookings
        from metrics            
    ),

    -- Calculate the differences and growth
    calculated_diff as (
        select
            *,
			--Difference in total stay value.
            (avg_online_rev - avg_offline_rev) as revenue_diff_per_stay,
			-- Difference in Average Daily Rate
            (online_adr - offline_adr) as adr_diff,
			-- Projected revenue impact if all bookings were online
            (online_bookings * (avg_online_rev - avg_offline_rev)) as incremental_revenue_impact
        from proposal_metrics
    ),
    
    -- STEP 5: Final formatting with clear business-ready names
    final as (
        select
            global_total_bookings as "Total Bookings",
            online_bookings as "Online Bookings",
            round(avg_online_rev, 2) as "Avg Online Total Stay",
            round(avg_offline_rev, 2) as "Avg Offline Total Stay",
            round(revenue_diff_per_stay, 2) as "Revenue Lift per Stay",
            round(online_adr, 2) as "Online Average Daily Rate",
            round(offline_adr, 2) as "Offline Average Daily Rate",
            round(adr_diff, 2) as "Average Daily Rate Difference",
            round(current_total_revenue, 2) as "Current Total Revenue",
            round(incremental_revenue_impact, 2) as "Projected Revenue Lift",
            round((incremental_revenue_impact / current_total_revenue) * 100, 2) || '%' as "Growth %",
            round(current_total_revenue + incremental_revenue_impact, 2) as "Projected Total Revenue"
        from calculated_diff
    )

select * from final