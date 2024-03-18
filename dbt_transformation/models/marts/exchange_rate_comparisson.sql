select
    t.year,
    t.month,
    t.day,
    t.hour,
    SUM(e.exchange_dk1_de)  AS exchange_dk1_de,
    SUM(e.exchange_dk1_nl) AS  exchange_dk1_nl,
    SUM(e.exchange_dk1_gb) as exchange_dk1_gb,
    SUM(e.exchange_dk1_NO) as exchange_dk1_NO,
    SUM(e.exchange_dk1_se) as exchange_dk1_se
from  {{ ref('time_dimension') }} as t
join {{ ref('energy_fact') }} as e
on e.time_id=t.time_id
group by t.year,t.month,t.day,t.hour