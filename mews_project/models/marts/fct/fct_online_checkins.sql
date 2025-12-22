with

	base as (
		select *
		from  {{ ref('reservations_rates') }}
	),

	calendar as (
		select *
		from {{ref('dim_calendar')}}
	),

	final as (
		select
			base.created_utc,
			cal.weekday as created_day,	
			base.age_group,
			base.gender,
			base.nationality_code,
			base.rate_name,
			count(*) as total_reservations,
			sum(is_online_checkin) as total_online_checkin
		from base
		left join calendar cal
		on cast(base.created_utc as date) = cal.dates
		group by
			base.created_utc,
			cal.weekday,	
			base.age_group,
			base.gender,
			base.nationality_code,
			base.rate_name
		order by total_online_checkin desc
	)

select * from final