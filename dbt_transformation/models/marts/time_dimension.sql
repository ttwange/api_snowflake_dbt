
select
    distinct Minutes1UTC as time_id,
    extract(year from Minutes1UTC) as year,
    extract(month from Minutes1UTC) as month,
    extract(day from Minutes1UTC) as day,
    extract(hour from Minutes1UTC) as hour
from {{ ref('stg_energy') }}