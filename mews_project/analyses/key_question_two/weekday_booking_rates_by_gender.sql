select distinct on (created_day)
    created_day,
    'gender' as dimension_type,
    gender::varchar as value,
    sum(total_reservations) as amount_of_reservations,
    sum(total_online_checkin) as amount_of_online_checkin
from fct__online_checkins
group by created_day, gender
order by created_day, sum(total_online_checkin) desc