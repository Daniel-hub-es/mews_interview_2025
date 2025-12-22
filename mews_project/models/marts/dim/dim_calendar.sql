select 
	"Weekday" as weekday,
	to_date("Day"::varchar, 'DD/MM/YYYY') AS dates
from {{ source('external', 'generated_calendar') }}