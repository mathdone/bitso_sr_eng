select
	max(event_timestamp) as last_login
from
	event
where
	event_name = 'login'
    and user_id = '8625af362985c33b7525678f3536b1b1'