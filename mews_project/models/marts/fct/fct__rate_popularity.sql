with 

    reservations_rates as (
        select *
        from {{ ref('int__reservations_rates') }}
    ),
    
    final as (
    select
		-- Year Date
        extract(year from created_utc) as created_year_reservation,
        extract(month from created_utc) as created_month_reservation,
        extract(year from start_utc) as start_year_reservation,
        extract(month from start_utc) as start_month_reservation,
        -- Dimensions
        rate_name, 
        age_group,
        gender, 
        nationality_code,
        -- Number of records
        count(*) as total_reservations
    from reservations_rates
    group by
        -- group by clause for the aggregation
        rate_name, 
        age_group,
        gender, 
        nationality_code,
		created_year_reservation,
        created_month_reservation,
        start_year_reservation,
        start_month_reservation
        -- order records from the greatest to the lowest amount
    order by total_reservations desc
    )

select * from final