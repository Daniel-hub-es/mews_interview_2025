select DISTINCT ON (gender)
	rate_name, 
	gender, 
	count(*) as booking_rate_volume
from fct__rate_popularity
group by rate_name, gender
order by gender, booking_rate_volume DESC;