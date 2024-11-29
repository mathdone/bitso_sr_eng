select
	user_id
from
	deposit
where
	event_timestamp <= '2021-07-05'
group by
	user_id
having
	count(*) > 5