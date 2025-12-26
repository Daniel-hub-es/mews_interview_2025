with 

    reservations_rates as (
        select *
        from {{ ref('int__reservations_rates') }}
    ),

    calendar as (
        select *
        from {{ ref('dim__calendar') }}
    ),
    
    final as (
    select
		-- Year Date
        created_utc,
		cal.year as created_year_reservation,
        cal.month,
        cal.week_number,
        -- Dimensions
        rr.rate_name, 
        rr.age_group,
        rr.gender, 
        rr.nationality_code,
        -- Number of records
        count(*) as total_reservations
    from reservations_rates rr
	left join calendar cal
	on cast(rr.created_utc as date) = cal.dates
    group by
        -- group by clause for the aggregation
        rate_name, 
        age_group,
        gender, 
        nationality_code,
		year,
        month,
        week_number,
        created_utc
        -- order records from the greatest to the lowest amount
    order by total_reservations desc
    )

select * from final