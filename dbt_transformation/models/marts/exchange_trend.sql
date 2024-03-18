select
    t.year,
    t.month,
    t.day,
    t.hour,
    f.Exchange_Sum,
    f.Exchange_DK1_DE,
    f.Exchange_DK1_NL,
    f.Exchange_DK1_GB,
    f.Exchange_DK1_NO,
    f.Exchange_DK1_SE,
    f.Exchange_DK1_DK2,
    f.Exchange_DK2_DE,
    f.Exchange_DK2_SE,
    f.Exchange_Bornholm_SE
from  {{ ref('time_dimension') }} as t
join {{ ref('energy_fact') }} as f
on f.time_id=t.time_id
