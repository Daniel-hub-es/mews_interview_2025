with

	rates as (
		select *
		from {{ ref('stg__mews_rates') }}
	),

	reservations as (
		select *
		from {{ ref('stg__mews_reservations') }}
	),

	reservations_rates as (
		select
			res.*,
			rates.rate_name,
			rates.short_rate_name,
			rates.settlement_action,
			rates.settlement_trigger,
			rates.settlement_value
		from reservations as res
		left join rates
		on res.rate_id = rates.rate_id
	)

select * from reservations_rates