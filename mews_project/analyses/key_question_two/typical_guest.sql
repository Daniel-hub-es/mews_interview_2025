select 
	age_group,
	case
		when gender = 0
		then 'undefined'
		when gender = 1
		then 'male'
		when gender = 2
		then 'female'
	end as gender,
	nationality_code,
	sum(total_online_checkin) as amount_of_online_checkin
from fct__online_checkins
group by age_group, gender, nationality_code, total_online_checkin
order by sum(total_online_checkin) desc
limit 1