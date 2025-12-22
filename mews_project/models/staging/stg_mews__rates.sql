with

    source as (
        select * 
        from {{ source('mews', 'rates') }}
    ),

    rates as (
        select
            cast(nullif(trim("RateId"), '') as varchar) as rate_id,
            cast(nullif(trim("RateName"), '') as varchar) as rate_name,
            cast(nullif(trim("ShortRateName"), '') as varchar) as short_rate_name,
            "SettlementAction" as settlement_action,
            "SettlementTrigger" as settlement_trigger,
            "SettlementValue" as settlement_value,
            "SettlementType" as settlement_type
        from source
    )

    select * from rates