select user_id, "level", jurisdiction
from (
	select
		user_id,
        "level",
        jurisdiction,
        row_number() over (partition by user_id, jurisdiction order by event_timestamp desc) as rn 
	from
		user_level	
) by_ts
where rn = 1