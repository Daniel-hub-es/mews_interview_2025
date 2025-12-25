select DISTINCT ON (age_group)
    rate_name, 
    age_group, 
    count(*) as booking_rate_volume
from fct__rate_popularity
group by rate_name, age_group
order by age_group, booking_rate_volume DESC