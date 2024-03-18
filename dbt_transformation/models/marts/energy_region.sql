select
    t.year,
    t.month,
    t.day,
    t.hour,
    SUM(e.solarpower + e.offshorewindpower + e.onshorewindpower) as renewable_energy,
    SUM(e.exchange_sum) as exchange_sum
from  {{ ref('time_dimension') }} as t
join {{ ref('energy_fact') }} as e
on e.time_id=t.time_id
group by t.year,t.month,t.day,t.hour