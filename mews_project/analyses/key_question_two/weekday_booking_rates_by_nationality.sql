select distinct on (created_day)
    created_day,
    'nationality_code' as dimension_type,
    nationality_code as value,
    sum(total_reservations) as amount_of_reservations,
    sum(total_online_checkin) as amount_of_online_checkin
from fct__online_checkins
group by created_day, nationality_code
order by created_day, sum(total_online_checkin) desc