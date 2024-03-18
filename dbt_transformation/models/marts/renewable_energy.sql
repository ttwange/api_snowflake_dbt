select
    t.year,
    t.month,
    t.day,
    t.hour,
    SUM(e.CO2Emission) as total_co2_emission,
    SUM(e.SolarPower) as SolarPower,
    SUM(e.Offshorewindpower) as Offshorewindpower,
    SUM(e.Onshorewindpower) as Onshorewindpower
from  {{ ref('time_dimension') }} as t
join {{ ref('energy_fact') }} as e
on e.time_id=t.time_id
group by t.year,t.month,t.day,t.hour