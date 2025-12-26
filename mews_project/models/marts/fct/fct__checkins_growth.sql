with

    base as (
        select *
        from {{ ref('int__reservations_rates') }}
    ),

    -- the following CTE creates two rows of data (online and desk checks)
    metrics as (
        select 
            is_online_checkin,
            count(*) as total_bookings,
            sum(night_cost_sum) as total_night_cost_sum,
            -- shows if digital guests spend more on average than on-site guests
            avg(night_cost_sum) as avg_night_cost_sum
        from base
        group by 1
    ),

    proposal_metrics as (
        select
                -- case check if the group of the resulting rows is an online guest or on-site guests
                -- collapsing into one single row by max() by column when the other result is 0 
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
                -- total revenue and global total bookings to compare in the final calculations
                sum(total_night_cost_sum) as total_revenue,
                sum(total_bookings) as global_total_bookings
            from metrics			
    ),

    calculated_diff as (
        select
            -- metrics
            global_total_bookings,
            online_bookings,
            avg_online_rev,
            avg_offline_rev,
            -- calculate the difference between online and ofline rev
            (avg_online_rev - avg_offline_rev) as avg_revenue_diff,
            -- total current revenue
            total_revenue as current_total_revenue,
            -- the incremental gain by bookings * extra value per booking calculation to show extra revenue
            online_bookings * (avg_online_rev - avg_offline_rev) as online_projected_growth_revenue,
            -- percentage revenue growth calculation using online_projected_growth_revenue calculated before
           
                ((online_bookings * (avg_online_rev - avg_offline_rev) / total_revenue) * 100) as percentage_revenue_growth,
            -- sum existing revenue and the extra revenue from moving from lower spender to higher spender (online checkins)
            total_revenue + (online_bookings * (avg_online_rev - avg_offline_rev)) as projected_total_revenue
        from proposal_metrics
    ),
    
	final as (
		select
			global_total_bookings,
            online_bookings,
			round(avg_online_rev, 3) as avg_online_rev,
			round(avg_offline_rev, 3) as avg_offline_rev,
			round(avg_revenue_diff, 3) as avg_revenue_diff,
			round(current_total_revenue, 3) as current_total_revenue,
			round(online_projected_growth_revenue, 3) as online_projected_growth_revenue,
			round(percentage_revenue_growth, 2) as percentage_revenue_growth,
			round(projected_total_revenue, 3) as projected_total_revenue
		from calculated_diff
	)

select * from final