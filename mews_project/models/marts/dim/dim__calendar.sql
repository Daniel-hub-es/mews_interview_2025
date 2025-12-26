select 
	"Weekday" as weekday,
	to_date("Day"::varchar, 'DD/MM/YYYY') AS dates,
	"Week_Number" as week_number,
	"Month" as month,
	"Year" as year
from {{ source('external', 'generated_calendar') }}