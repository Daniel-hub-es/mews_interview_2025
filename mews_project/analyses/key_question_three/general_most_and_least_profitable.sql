with 
    segment_metrics as (
        select 
            age_group,
            gender,
            nationality_code,
            count(*) as total_reservations,
            sum(night_cost_sum) as total_rev,
            sum(night_count) as total_nights,
            sum(occupied_space_sum) as total_capacity_used,
            sum(guest_count_sum) as total_guests,
            round(sum(night_cost_sum) / nullif(sum(night_count), 0), 3) as adr,
            round(cast(sum(night_count) as decimal) / count(*), 1) as avg_los,
            round(cast(sum(guest_count_sum) as decimal) / count(*), 1) as avg_occupancy,
            round(
                sum(night_cost_sum) / nullif(sum(occupied_space_sum), 0), 
            3) as avg_rev_per_capacity_unit
        from fct__revenue
        group by 1, 2, 3
    ),

    ranked_segments as (
        select 
            *,
            rank() over(order by avg_rev_per_capacity_unit desc) as high_rank,
            rank() over(order by avg_rev_per_capacity_unit asc) as low_rank
        from segment_metrics
        where avg_rev_per_capacity_unit > 0
    ),

    final as (
        select 
            gender as "Gender",
            age_group as "Age Group",
            nationality_code as "Nationality Code",
            total_reservations as "Total Reservations",
            adr as "Average Daily Rate",
            avg_los as "Average Length of Stay",
            avg_occupancy as "Average Occupancy per Booking",
            avg_rev_per_capacity_unit as "Revenue per Space Unit",
            case 
                when high_rank = 1 then 'MOST'
                when low_rank = 1 then 'LEAST'
            end as "Profitability"
        from ranked_segments
        where high_rank = 1 or low_rank = 1
        order by avg_rev_per_capacity_unit desc
    )

select * from final;