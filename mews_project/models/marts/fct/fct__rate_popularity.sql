with 

    reservations_rates as (
        select *
        from {{ ref('int__reservations_rates') }}
    ),
    
    rate_popularity as (
    select
        rate_name, 
        age_group,
        gender, 
        nationality_code, 
        count(*) as total_reservations
    from reservations_rates
    group by
        rate_name, 
        age_group,
        gender, 
        nationality_code
    order by total_reservations desc
    )

select * from rate_popularity