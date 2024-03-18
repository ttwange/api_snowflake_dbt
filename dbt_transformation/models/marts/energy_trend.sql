select
    t.year,
    t.month,
    t.day,
    t.hour,
    f.CO2Emission,
    f.ProductionGe100MW,
    f.ProductionLt100MW
from  {{ ref('time_dimension') }} as t
join {{ ref('energy_fact') }} as f
on f.time_id=t.time_id