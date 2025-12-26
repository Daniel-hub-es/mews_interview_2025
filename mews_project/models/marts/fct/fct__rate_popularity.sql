with 

    reservations_rates as (
        select *
        from {{ ref('int__reservations_rates') }}
    ),
    
    rate_popularity as (
    select
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
        nationality_code
        -- order records from the greatest to the lowest amount
    order by total_reservations desc
    )

select * from rate_popularity