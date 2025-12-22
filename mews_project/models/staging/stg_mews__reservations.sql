with

    source as (
        select * 
        from {{ source('mews', 'reservations') }}
    ),

    reservations as (
        select
            cast(nullif(trim("StartUtc"), '') as timestamp without time zone) as start_utc,
            cast(nullif(trim("EndUtc"), '') as timestamp without time zone) as end_utc,
            cast(nullif(trim("CreatedUtc"), '') as timestamp without time zone) as created_utc,
            "NightCount" as night_count,
            cast("NightCost_Sum" as decimal) as night_cost_sum,
            "OccupiedSpace_Sum" as occupied_space_sum,
            "GuestCount_Sum" as guest_count_sum,
            "LeadTime" as lead_time,
            "StayLength" as stay_length,
            "CancellationReason" as cancellation_reason,
            "SettlementType" as settlement_type,
            "ReservationState" as reservation_state,
            "Origin" as origin,
            "CommanderOrigin" as commander_origin,
            cast(nullif(trim("TravelAgency"), '') as varchar) as travel_agency,
            "IsOnlineCheckin"as is_online_checkin,
            cast(nullif(trim("NationalityCode"), '') as varchar) as nationality_code,
            "Gender" as gender,
            "Classification" as classification,
            "AgeGroup" as age_group,
            "HasEmail" as has_email,
            cast(nullif(trim("BusinessSegment"), '') as varchar) as business_segment,
            "Tier" as tier,
            cast(nullif(trim("RateId"), '') as varchar) as rate_id
        from source
    )

    select * from reservations