select
	sum(amount)
from
	deposit
where
	currency = 'ars'
	and date(event_timestamp) = '2022-11-25'
